# REST API for change control. Reads and plain edits go through NetBox's viewsets;
# everything with a rule (state, review, scope, evidence, close) is an action that
# calls the same service the web page calls, so the two refuse the same things.
#
# Actions check their model permission by hand, as the web views do, rather than
# through the permission a POST would otherwise map to (`add`), which does not
# describe "move this change on" and would restrict the queryset to nothing for a
# user who may change but not add.
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404
from drf_spectacular.utils import extend_schema
from netbox.api.authentication import TokenWritePermission
from netbox.api.viewsets import NetBoxModelViewSet
from netbox.api.viewsets import NetBoxReadOnlyModelViewSet
from rest_framework import serializers
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from .change_serializers import ForwardChangeApprovalRuleSerializer
from .change_serializers import ForwardChangeCommentSerializer
from .change_serializers import ForwardChangeCriterionSerializer
from .change_serializers import ForwardChangeEvidenceSerializer
from .change_serializers import ForwardChangePolicyRuleSerializer
from .change_serializers import ForwardChangePolicySerializer
from .change_serializers import ForwardChangeReviewSerializer
from .change_serializers import ForwardChangeSerializer
from forward_netbox.filtersets import ForwardChangeApprovalRuleFilterSet
from forward_netbox.filtersets import ForwardChangeCommentFilterSet
from forward_netbox.filtersets import ForwardChangeCriterionFilterSet
from forward_netbox.filtersets import ForwardChangeEvidenceFilterSet
from forward_netbox.filtersets import ForwardChangeFilterSet
from forward_netbox.filtersets import ForwardChangePolicyFilterSet
from forward_netbox.filtersets import ForwardChangePolicyRuleFilterSet
from forward_netbox.filtersets import ForwardChangeReviewFilterSet
from forward_netbox.models import ForwardChange
from forward_netbox.models import ForwardChangeApprovalRule
from forward_netbox.models import ForwardChangeComment
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeEvidence
from forward_netbox.models import ForwardChangePolicy
from forward_netbox.models import ForwardChangePolicyRule
from forward_netbox.models import ForwardChangeReview


class ActionResultSerializer(serializers.Serializer):
    ok = serializers.BooleanField()
    detail = serializers.CharField(allow_blank=True)
    reasons = serializers.ListField(child=serializers.CharField())


class _NoBody(serializers.Serializer):
    pass


class _TransitionBody(serializers.Serializer):
    target = serializers.CharField()


class _ReviewBody(serializers.Serializer):
    decision = serializers.ChoiceField(choices=["approve", "reject"])
    anchor = serializers.CharField(
        help_text="The change's `review_anchor` as the reviewer saw it."
    )
    comment = serializers.CharField(required=False, allow_blank=True)


class _AttestBody(serializers.Serializer):
    applied_ref = serializers.CharField(required=False, allow_blank=True)


class _NamesBody(serializers.Serializer):
    names = serializers.ListField(
        child=serializers.CharField(), min_length=1, max_length=500
    )


class _RowBody(serializers.Serializer):
    row_id = serializers.IntegerField()


class _AdoptBody(serializers.Serializer):
    network_id = serializers.CharField()


class _CommentBody(serializers.Serializer):
    body = serializers.CharField()


class _ReplyBody(serializers.Serializer):
    comment_id = serializers.IntegerField()
    body = serializers.CharField()


class _ResolveBody(serializers.Serializer):
    comment_id = serializers.IntegerField()
    resolved = serializers.BooleanField(default=True)


class _Refused:
    ok = False

    def __init__(self, message):
        self.message = message


def _reply(outcome, **extra):
    """An outcome as a response: 200 when it worked, 409 with the reasons if not."""
    ok = bool(getattr(outcome, "ok", outcome))
    detail = getattr(outcome, "message", "") or ""
    reasons = list(
        getattr(outcome, "blockers", ()) or getattr(outcome, "reasons", ()) or ()
    )
    reasons += list(getattr(outcome, "problems", ()) or ())
    body = {"ok": ok, "detail": detail, "reasons": reasons, **extra}
    return Response(body, status=status.HTTP_200_OK if ok else status.HTTP_409_CONFLICT)


class ForwardChangeViewSet(NetBoxModelViewSet):
    queryset = ForwardChange.objects.select_related(
        "source", "branch", "requester"
    ).prefetch_related("devices__device")
    serializer_class = ForwardChangeSerializer
    filterset_class = ForwardChangeFilterSet

    def perform_create(self, serializer):
        serializer.validated_data["requester"] = self.request.user
        super().perform_create(serializer)

    def _act(
        self, request, pk, permission, call, body_class=_NoBody, restrict="change"
    ):
        """Check the permission, find the change among those the user may reach by
        that permission (so object-level constraints hold), validate the body and
        call the service."""
        if not request.user.has_perm(permission):
            raise PermissionDenied(f"Requires {permission}.")
        change = get_object_or_404(
            ForwardChange.objects.restrict(request.user, restrict), pk=pk
        )
        body = body_class(data=request.data)
        body.is_valid(raise_exception=True)
        return call(change, body.validated_data)

    CHANGE = "forward_netbox.change_forwardchange"

    @extend_schema(request=_TransitionBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="transition",
    )
    def transition(self, request, pk=None):
        from forward_netbox.change_control.actions import transition
        from forward_netbox.change_control.state_machine import manual_transitions

        def call(change, data):
            target = data["target"]
            if target not in manual_transitions(change.state):
                return _reply(_Refused("That move is not one you can make by hand."))
            outcome = transition(change, target, request.user)
            return _reply(outcome, state=outcome.state)

        return self._act(request, pk, self.CHANGE, call, _TransitionBody)

    @extend_schema(request=_AttestBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="attest-applied",
    )
    def attest_applied(self, request, pk=None):
        from forward_netbox.change_control.actions import attest_applied

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(
                attest_applied(
                    change,
                    request.user,
                    ref=(data.get("applied_ref") or "").strip()[:200],
                )
            ),
            _AttestBody,
        )

    @extend_schema(request=_ReviewBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="review",
    )
    def review(self, request, pk=None):
        from forward_netbox.change_control.actions import record_review

        def call(change, data):
            outcome = record_review(
                change,
                request.user,
                approved=data["decision"] == "approve",
                comment=data.get("comment"),
                anchor=data["anchor"],
            )
            return _reply(outcome)

        return self._act(
            request,
            pk,
            "forward_netbox.approve_forwardchange",
            call,
            _ReviewBody,
            restrict="approve",
        )

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="close",
    )
    def close(self, request, pk=None):
        from forward_netbox.change_control.actions import close_change

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(close_change(change, request.user)),
        )

    @extend_schema(request=_NamesBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="devices/add",
    )
    def devices_add(self, request, pk=None):
        from forward_netbox.change_control.scope import add_devices
        from forward_netbox.change_control.scope import parse_device_names

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(
                add_devices(
                    change,
                    parse_device_names("\n".join(data["names"])),
                    request.user,
                )
            ),
            _NamesBody,
        )

    @extend_schema(request=_RowBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="devices/remove",
    )
    def devices_remove(self, request, pk=None):
        from forward_netbox.change_control.scope import remove_device

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(
                remove_device(change, data["row_id"], request.user)
            ),
            _RowBody,
        )

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="devices/resolve",
    )
    def devices_resolve(self, request, pk=None):
        from forward_netbox.change_control.scope import resolve_forward_names

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(resolve_forward_names(change, request.user)),
        )

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="workspace/create",
    )
    def workspace_create(self, request, pk=None):
        from forward_netbox.change_control.workspace import create_workspace

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(create_workspace(change, request.user)),
        )

    @extend_schema(request=_AdoptBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="workspace/adopt",
    )
    def workspace_adopt(self, request, pk=None):
        from forward_netbox.change_control.workspace import adopt_workspace

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(
                adopt_workspace(change, data["network_id"], request.user)
            ),
            _AdoptBody,
        )

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="workspace/refresh",
    )
    def workspace_refresh(self, request, pk=None):
        from forward_netbox.change_control.workspace import refresh_snapshots

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(refresh_snapshots(change)),
        )

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="evidence/baseline",
    )
    def evidence_baseline(self, request, pk=None):
        from forward_netbox.change_control.choices import ForwardEvidencePhaseChoices
        from forward_netbox.change_control.runner import record_evidence

        return self._act(
            request,
            pk,
            self.CHANGE,
            lambda change, data: _reply(
                record_evidence(change, ForwardEvidencePhaseChoices.BEFORE)
            ),
        )

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="verify",
    )
    def verify(self, request, pk=None):
        from forward_netbox.change_control.runner import verify_change

        def call(change, data):
            outcome = verify_change(change, request.user)
            return _reply(outcome, verdict=getattr(outcome, "verdict", ""))

        return self._act(request, pk, self.CHANGE, call)

    @extend_schema(request=_CommentBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="comment",
    )
    def comment(self, request, pk=None):
        from forward_netbox.change_control.comments import add_comment

        return self._act(
            request,
            pk,
            "forward_netbox.add_forwardchangecomment",
            lambda change, data: _reply(
                add_comment(change, request.user, data["body"])
            ),
            _CommentBody,
            restrict="view",
        )

    @extend_schema(request=_ReplyBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="reply",
    )
    def reply(self, request, pk=None):
        from forward_netbox.change_control.comments import reply_to_comment

        def call(change, data):
            comment = get_object_or_404(change.change_comments, pk=data["comment_id"])
            return _reply(reply_to_comment(comment, request.user, data["body"]))

        return self._act(
            request,
            pk,
            "forward_netbox.add_forwardchangecommentreply",
            call,
            _ReplyBody,
            restrict="view",
        )

    @extend_schema(request=_ResolveBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="resolve",
    )
    def resolve(self, request, pk=None):
        from forward_netbox.change_control.comments import set_resolved

        def call(change, data):
            comment = get_object_or_404(change.change_comments, pk=data["comment_id"])
            return _reply(set_resolved(comment, request.user, data["resolved"]))

        return self._act(
            request,
            pk,
            "forward_netbox.change_forwardchangecomment",
            call,
            _ResolveBody,
            restrict="view",
        )


class ForwardChangePolicyViewSet(NetBoxModelViewSet):
    queryset = ForwardChangePolicy.objects.all()
    serializer_class = ForwardChangePolicySerializer
    filterset_class = ForwardChangePolicyFilterSet


class ForwardChangePolicyRuleViewSet(NetBoxModelViewSet):
    queryset = ForwardChangePolicyRule.objects.select_related("policy")
    serializer_class = ForwardChangePolicyRuleSerializer
    filterset_class = ForwardChangePolicyRuleFilterSet


class ForwardChangeApprovalRuleViewSet(NetBoxModelViewSet):
    queryset = ForwardChangeApprovalRule.objects.select_related(
        "policy"
    ).prefetch_related("groups", "reviewers")
    serializer_class = ForwardChangeApprovalRuleSerializer
    filterset_class = ForwardChangeApprovalRuleFilterSet


class ForwardChangeCriterionViewSet(NetBoxModelViewSet):
    queryset = ForwardChangeCriterion.objects.select_related("change")
    serializer_class = ForwardChangeCriterionSerializer
    filterset_class = ForwardChangeCriterionFilterSet

    # The freeze is re-checked with the change locked: the serializer checked it
    # without a lock, and a staging or an evidence write may have landed since.
    def _locked(self, change_id, criterion_pk=None):
        from forward_netbox.change_control.criteria_admin import CriterionRefused
        from forward_netbox.change_control.criteria_admin import locked_for_edit

        class _Guard:
            def __enter__(inner):
                inner.cm = locked_for_edit(change_id, criterion_pk)
                try:
                    return inner.cm.__enter__()
                except CriterionRefused as exc:
                    raise serializers.ValidationError(str(exc))

            def __exit__(inner, *exc_info):
                try:
                    return inner.cm.__exit__(*exc_info)
                except CriterionRefused as exc:
                    raise serializers.ValidationError(str(exc))

        return _Guard()

    def perform_create(self, serializer):
        with self._locked(serializer.validated_data["change"].pk):
            super().perform_create(serializer)

    def perform_update(self, serializer):
        with self._locked(serializer.instance.change_id, serializer.instance.pk):
            super().perform_update(serializer)

    def perform_destroy(self, instance):
        from forward_netbox.change_control.actions import _journal

        with self._locked(instance.change_id, instance.pk) as change:
            name = instance.name
            super().perform_destroy(instance)
            _journal(change, self.request.user, f"Removed criterion '{name}'.")

    @extend_schema(request=_NoBody, responses={200: ActionResultSerializer})
    @action(
        detail=True,
        methods=["post"],
        permission_classes=[TokenWritePermission],
        url_path="bind",
    )
    def bind(self, request, pk=None):
        from forward_netbox.change_control.criteria_admin import bind

        if not request.user.has_perm("forward_netbox.change_forwardchangecriterion"):
            raise PermissionDenied("Requires change_forwardchangecriterion.")
        criterion = get_object_or_404(
            ForwardChangeCriterion.objects.restrict(request.user, "change"), pk=pk
        )
        return _reply(bind(criterion.change, criterion.pk, request.user))


class ForwardChangeEvidenceViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardChangeEvidence.objects.select_related("criterion__change")
    serializer_class = ForwardChangeEvidenceSerializer
    filterset_class = ForwardChangeEvidenceFilterSet


class ForwardChangeReviewViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardChangeReview.objects.select_related("change", "reviewer")
    serializer_class = ForwardChangeReviewSerializer
    filterset_class = ForwardChangeReviewFilterSet


class ForwardChangeCommentViewSet(NetBoxReadOnlyModelViewSet):
    queryset = ForwardChangeComment.objects.select_related(
        "change", "author", "resolved_by"
    ).prefetch_related("replies__author")
    serializer_class = ForwardChangeCommentSerializer
    filterset_class = ForwardChangeCommentFilterSet
