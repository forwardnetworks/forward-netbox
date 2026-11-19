from django import forms
from django.utils.html import escapejs
from django.utils.html import format_html
from django.utils.safestring import mark_safe

# The endpoint the device-tag filters already use to list the tags Forward has.
AVAILABLE_TAGS_URL = "/api/plugins/forward/source/available-tags/"

_BUILDER = """
<div class="border rounded p-2 mt-2" data-forward-site-rule-builder="{field_id}"
     data-source-id="{source_id}" data-tags-url="{tags_url}">
  <div class="fw-bold mb-1">Add a rule</div>
  <div class="row g-2 align-items-end">
    <div class="col-12 col-md-3">
      <label class="form-label mb-0">Match a device by its</label>
      <select class="form-select" data-kind>
        <option value="location">Forward location</option>
        <option value="tag">Forward tag</option>
      </select>
    </div>
    <div class="col-12 col-md-4">
      <label class="form-label mb-0" data-value-label>Location (a pattern, e.g. DC01*)</label>
      <input type="text" class="form-control" data-location placeholder="DC01*">
      <select class="form-select" data-tag hidden></select>
    </div>
    <div class="col-12 col-md-3">
      <label class="form-label mb-0">NetBox site</label>
      <input type="text" class="form-control" data-site placeholder="dc1">
    </div>
    <div class="col-12 col-md-2">
      <button type="button" class="btn btn-outline-primary w-100" data-add>Add rule</button>
    </div>
  </div>
  <div class="form-text" data-status>{status}</div>
</div>
"""

# Progressive enhancement: the textarea above is the field and works without this.
# The script lets an operator pick a Forward tag from a list loaded from Forward
# instead of typing it, and appends the rule as a line.
_SCRIPT = """
<script>
(function () {
  var builder = document.querySelector('[data-forward-site-rule-builder="__ID__"]');
  var area = document.getElementById('__ID__');
  if (!builder || !area) { return; }
  var kind = builder.querySelector('[data-kind]');
  var loc = builder.querySelector('[data-location]');
  var tag = builder.querySelector('[data-tag]');
  var site = builder.querySelector('[data-site]');
  var label = builder.querySelector('[data-value-label]');
  var status = builder.querySelector('[data-status]');
  var loaded = false;
  function show() {
    var isTag = kind.value === 'tag';
    loc.hidden = isTag;
    tag.hidden = !isTag;
    label.textContent = isTag ? 'Forward tag' : 'Location (a pattern, e.g. DC01*)';
    if (isTag && !loaded) { loadTags(); }
  }
  function loadTags() {
    loaded = true;
    var id = builder.getAttribute('data-source-id');
    if (!id) {
      status.textContent = 'Save the source to pick tags from Forward; until then type the tag name.';
      swap(false);
      return;
    }
    status.textContent = 'Loading tags from Forward...';
    fetch(builder.getAttribute('data-tags-url') + '?source_id=' + encodeURIComponent(id), {
      credentials: 'same-origin', headers: {'Accept': 'application/json'}
    }).then(function (r) { return r.json(); }).then(function (body) {
      var tags = body.results || [];
      if (!tags.length) {
        status.textContent = body.detail || 'Forward returned no tags; type the tag name.';
        swap(false);
        return;
      }
      tag.innerHTML = '';
      tags.forEach(function (t) {
        var o = document.createElement('option');
        o.value = t.id; o.textContent = t.display || t.id;
        tag.appendChild(o);
      });
      status.textContent = tags.length + ' tags from Forward.';
    }).catch(function () {
      status.textContent = 'Could not load tags from Forward; type the tag name.';
      swap(false);
    });
  }
  function swap(useSelect) {
    if (useSelect) { return; }
    var input = document.createElement('input');
    input.type = 'text'; input.className = 'form-control';
    input.setAttribute('data-tag', '');
    tag.parentNode.replaceChild(input, tag);
    tag = input;
  }
  kind.addEventListener('change', show);
  builder.querySelector('[data-add]').addEventListener('click', function () {
    var value = (kind.value === 'tag' ? tag.value : loc.value).trim();
    var name = site.value.trim();
    if (!value || !name) { status.textContent = 'Fill in what to match and the NetBox site.'; return; }
    var line = kind.value + ' ' + value + ' -> ' + name;
    area.value = (area.value.trim() ? area.value.replace(/\\s+$/, '') + '\\n' : '') + line + '\\n';
    loc.value = ''; site.value = '';
    status.textContent = 'Added: ' + line + '. Save the source to apply it.';
  });
  show();
})();
</script>
"""


class SiteRulesWidget(forms.Textarea):
    """The site mapping rules: one rule per line, with a builder beneath.

    The textarea is the field and works on its own. The builder is an aid: for a tag
    rule it lists the tags Forward has, so a tag is picked and never mistyped.
    """

    def __init__(self, attrs=None):
        defaults = {
            "rows": 5,
            "spellcheck": "false",
            "class": "form-control font-monospace",
        }
        super().__init__({**defaults, **(attrs or {})})
        self.source_id = None

    def render(self, name, value, attrs=None, renderer=None):
        area = super().render(name, value, attrs, renderer)
        field_id = (attrs or {}).get("id") or self.attrs.get("id") or f"id_{name}"
        if self.source_id:
            status = ""
        else:
            status = "Save the source to pick tags from Forward."
        builder = format_html(
            _BUILDER,
            field_id=field_id,
            source_id=self.source_id or "",
            tags_url=AVAILABLE_TAGS_URL,
            status=status,
        )
        script = mark_safe(_SCRIPT.replace("__ID__", escapejs(str(field_id))))
        return mark_safe(f"{area}{builder}{script}")
