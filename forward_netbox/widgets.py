from django import forms
from django.utils.html import escapejs
from django.utils.html import format_html
from django.utils.html import format_html_join
from django.utils.safestring import mark_safe

# The endpoint the device-tag filters already use to list the tags Forward has.
AVAILABLE_TAGS_URL = "/api/plugins/forward/source/available-tags/"
# And the one that lists its locations, offered as suggestions for a location rule.
AVAILABLE_LOCATIONS_URL = "/api/plugins/forward/source/available-locations/"

_TABLE = """
<div class="mt-2" data-forward-site-rule-table="{field_id}" hidden>
  <table class="table table-sm align-middle mb-1">
    <thead><tr>
      <th>Match a device by its</th><th>Forward location or tag</th>
      <th>NetBox site</th><th></th>
    </tr></thead>
    <tbody data-rows></tbody>
  </table>
  <datalist id="{field_id}_sites">{site_options}</datalist>
  <button type="button" class="btn btn-sm btn-outline-primary" data-add-row>Add row</button>
  <button type="button" class="btn btn-sm btn-outline-secondary" data-edit-text>Edit as text</button>
  <div class="form-text text-danger" data-table-status></div>
  <span class="form-text d-block">Rules apply top to bottom; the first match wins. A NetBox_Site_ tag on a device beats every rule.</span>
</div>
"""

_BUILDER = """
<div class="border rounded p-2 mt-2" data-forward-site-rule-builder="{field_id}"
     data-source-id="{source_id}" data-tags-url="{tags_url}"
     data-locations-url="{locations_url}">
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
      <input type="text" class="form-control" data-location placeholder="DC01*"
             list="{field_id}_locations" autocomplete="off">
      <datalist id="{field_id}_locations" data-location-options></datalist>
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
  // A location rule is a pattern, so the field stays free text; the names Forward
  // has are only suggestions (a datalist), loaded once for a saved source.
  var locationsLoaded = false;
  function loadLocations() {
    if (locationsLoaded) { return; }
    locationsLoaded = true;
    var id = builder.getAttribute('data-source-id');
    var options = builder.querySelector('[data-location-options]');
    if (!id || !options) { return; }
    fetch(builder.getAttribute('data-locations-url') + '?source_id=' + encodeURIComponent(id), {
      credentials: 'same-origin', headers: {'Accept': 'application/json'}
    }).then(function (r) { return r.json(); }).then(function (body) {
      (body.results || []).forEach(function (l) {
        var o = document.createElement('option');
        o.value = l.id;
        options.appendChild(o);
      });
    }).catch(function () { /* suggestions are optional */ });
  }
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
    tagsOnce().then(function (res) {
      if (!res.tags.length) {
        status.textContent = res.detail || 'Forward returned no tags; type the tag name.';
        swap(false);
        return;
      }
      tag.innerHTML = '';
      res.tags.forEach(function (t) {
        var o = document.createElement('option');
        o.value = t; o.textContent = t;
        tag.appendChild(o);
      });
      status.textContent = res.tags.length + ' tags from Forward.';
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
  // Not on every form view: that would ask Forward twice before anyone had touched
  // a location rule. The first time the field is used is soon enough.
  loc.addEventListener('focus', loadLocations, {once: true});
  builder.querySelector('[data-add]').addEventListener('click', function () {
    var value = (kind.value === 'tag' ? tag.value : loc.value).trim();
    var name = site.value.trim();
    if (!value || !name) { status.textContent = 'Fill in what to match and the NetBox site.'; return; }
    var line = kind.value + ' ' + value + ' -> ' + name;
    if (table && !table.hidden) {
      addRow({kind: kind.value, value: value, site: name});
      writeBack();
    } else {
      area.value = (area.value.trim() ? area.value.replace(/\\s+$/, '') + '\\n' : '') + line + '\\n';
    }
    loc.value = ''; site.value = '';
    status.textContent = 'Added: ' + line + '. Save the source to apply it.';
  });
  show();

  // The rules as a table: a row per rule, with the same pickers as the builder. The
  // textarea stays the field that is posted, rewritten from the rows when a row is
  // changed by hand, and stays the only editor when a line is not one the table can show.
  var table = document.querySelector('[data-forward-site-rule-table="__ID__"]');
  var tableStatus = table && table.querySelector('[data-table-status]');
  var tagResult = null;
  // Asked once, and only when someone first uses a tag control: the query reads the
  // whole network.
  function tagsOnce() {
    if (tagResult) { return tagResult; }
    var id = builder.getAttribute('data-source-id');
    if (!id) {
      tagResult = Promise.resolve({tags: [], detail: ''});
      return tagResult;
    }
    tagResult = fetch(builder.getAttribute('data-tags-url') + '?source_id=' + encodeURIComponent(id), {
      credentials: 'same-origin', headers: {'Accept': 'application/json'}
    }).then(function (r) { return r.json(); }).then(function (b) {
      return {tags: (b.results || []).map(function (t) { return t.id; }), detail: b.detail || ''};
    }).catch(function () {
      return {tags: [], detail: 'Could not load tags from Forward; type the tag name.'};
    });
    return tagResult;
  }
  // The same reading of a line as the server's: the kind is the first word, the
  // first ' -> ' splits the rest.
  function parseLines(text) {
    var rules = [];
    var lines = text.split('\\n');
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (!line) { continue; }
      var sp = line.indexOf(' ');
      if (sp < 1) { return null; }
      var kindName = line.slice(0, sp);
      var rest = line.slice(sp + 1);
      var arrow = rest.indexOf(' -> ');
      if (arrow < 0 || (kindName !== 'location' && kindName !== 'tag')) { return null; }
      var value = rest.slice(0, arrow).trim();
      var siteName = rest.slice(arrow + 4).trim();
      if (!value || !siteName) { return null; }
      rules.push({kind: kindName, value: value, site: siteName});
    }
    return rules;
  }
  function rowsState() {
    var out = [];
    table.querySelectorAll('tr[data-rule]').forEach(function (tr) {
      out.push({
        kind: tr.querySelector('[data-rule-kind]').value,
        value: tr.querySelector('[data-value]').value.trim(),
        site: tr.querySelector('[data-rule-site]').value.trim()
      });
    });
    return out;
  }
  function incomplete() {
    return rowsState().filter(function (r) { return (r.value || r.site) && !(r.value && r.site); });
  }
  // Only a rule with both halves is written; a half-filled row stays in the table and
  // stops the save (see the submit handler) rather than being dropped on the way.
  function writeBack() {
    var lines = rowsState().filter(function (r) { return r.value && r.site; }).map(function (r) {
      return r.kind + ' ' + r.value + ' -> ' + r.site;
    });
    area.value = lines.length ? lines.join('\\n') + '\\n' : '';
    if (tableStatus) { tableStatus.textContent = ''; }
  }
  function textControl(current, kindValue) {
    var control = document.createElement('input');
    control.type = 'text'; control.className = 'form-control form-control-sm';
    control.autocomplete = 'off'; control.value = current || '';
    if (kindValue === 'location') {
      control.setAttribute('list', '__ID___locations'); control.placeholder = 'DC01*';
      control.addEventListener('focus', loadLocations, {once: true});
    } else {
      control.placeholder = 'Forward tag';
    }
    return control;
  }
  function valueControl(tr, kindValue, current) {
    var cell = tr.querySelector('[data-value-cell]');
    cell.innerHTML = '';
    var control;
    if (kindValue === 'tag' && builder.getAttribute('data-source-id')) {
      control = document.createElement('select');
      control.className = 'form-select form-select-sm';
      var blank = document.createElement('option');
      blank.value = ''; blank.textContent = 'Choose a tag'; control.appendChild(blank);
      if (current) {
        var cur = document.createElement('option');
        cur.value = current; cur.textContent = current; control.appendChild(cur);
        control.value = current;
      }
      var filled = false;
      var fill = function () {
        if (filled) { return; }
        filled = true;
        tagsOnce().then(function (res) {
          if (!res.tags.length) {
            // Nothing to pick from: type the tag, as the builder does.
            var input = textControl(control.value, 'tag');
            input.setAttribute('data-value', '');
            input.addEventListener('input', writeBack);
            if (control.parentNode) { control.parentNode.replaceChild(input, control); }
            return;
          }
          var chosen = control.value;
          res.tags.forEach(function (t) {
            if (t === current) { return; }
            var o = document.createElement('option'); o.value = t; o.textContent = t; control.appendChild(o);
          });
          control.value = chosen;
        });
      };
      // The list is fetched when the control is first used, not when the page opens.
      control.addEventListener('focus', fill, {once: true});
      control.addEventListener('mousedown', fill, {once: true});
    } else {
      control = textControl(current, kindValue);
    }
    control.setAttribute('data-value', '');
    control.addEventListener('input', writeBack);
    control.addEventListener('change', writeBack);
    cell.appendChild(control);
  }
  function addRow(rule) {
    var tr = document.createElement('tr');
    tr.setAttribute('data-rule', '');
    tr.innerHTML = '<td><select class="form-select form-select-sm" data-rule-kind>' +
      '<option value="location">Forward location</option><option value="tag">Forward tag</option></select></td>' +
      '<td data-value-cell></td>' +
      '<td><input type="text" class="form-control form-control-sm" data-rule-site list="__ID___sites" placeholder="dc1" autocomplete="off"></td>' +
      '<td><button type="button" class="btn btn-sm btn-outline-danger" data-remove title="Remove rule">&times;</button></td>';
    var k = tr.querySelector('[data-rule-kind]');
    k.value = rule.kind;
    tr.querySelector('[data-rule-site]').value = rule.site;
    valueControl(tr, rule.kind, rule.value);
    k.addEventListener('change', function () { valueControl(tr, k.value, ''); writeBack(); });
    tr.querySelector('[data-rule-site]').addEventListener('input', writeBack);
    tr.querySelector('[data-remove]').addEventListener('click', function () { tr.remove(); writeBack(); });
    table.querySelector('[data-rows]').appendChild(tr);
  }
  function showTable() {
    var rules = parseLines(area.value);
    if (!rules) { table.hidden = true; area.hidden = false; return false; }
    table.querySelector('[data-rows]').innerHTML = '';
    rules.forEach(addRow);
    table.hidden = false; area.hidden = true;
    return true;
  }
  if (table) {
    showTable();
    table.querySelector('[data-add-row]').addEventListener('click', function () {
      addRow({kind: 'location', value: '', site: ''});
    });
    table.querySelector('[data-edit-text]').addEventListener('click', function () {
      if (incomplete().length) {
        tableStatus.textContent = 'Finish or remove the half-filled row before switching to text.';
        return;
      }
      writeBack();
      table.hidden = true; area.hidden = false;
      var back = document.createElement('button');
      back.type = 'button'; back.className = 'btn btn-sm btn-outline-secondary mt-1';
      back.textContent = 'Show as table'; back.setAttribute('data-show-table', '');
      back.addEventListener('click', function () {
        if (showTable()) { back.remove(); }
        else { status.textContent = 'A line is not in the form <location|tag> <pattern> -> <site>; fix it to use the table.'; }
      });
      area.parentNode.insertBefore(back, area.nextSibling);
    });
    if (area.form) {
      area.form.addEventListener('submit', function (event) {
        if (table.hidden) { return; }
        if (incomplete().length) {
          event.preventDefault();
          tableStatus.textContent = 'A rule needs both what to match and the NetBox site; fill it in or remove the row.';
          table.scrollIntoView({block: 'center'});
        }
      });
    }
  }
})();
</script>
"""


def _site_options():
    """The NetBox sites that exist, as suggestions. A rule may name a site that does
    not exist yet (the sync creates it), so the field stays free text."""
    from dcim.models import Site

    try:
        names = Site.objects.order_by("name").values_list("name", flat=True)[:2000]
        return format_html_join("", '<option value="{}">', ((n,) for n in names))
    except Exception:  # suggestions are optional
        return ""


class SiteRulesWidget(forms.Textarea):
    """The site mapping rules: one rule per line, with a builder beneath.

    The textarea is the field and works on its own. The builder is an aid: for a tag
    rule it lists the tags Forward has, so a tag is picked and never mistyped.
    """

    def __init__(self, attrs=None):
        defaults = {
            "rows": 5,
            "spellcheck": "false",
            # The table is built from the value the server rendered; a browser that
            # restores older text into the box would leave the two disagreeing.
            "autocomplete": "off",
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
        table = format_html(
            _TABLE, field_id=field_id, site_options=_site_options()
        )
        builder = format_html(
            _BUILDER,
            field_id=field_id,
            source_id=self.source_id or "",
            tags_url=AVAILABLE_TAGS_URL,
            locations_url=AVAILABLE_LOCATIONS_URL,
            status=status,
        )
        script = mark_safe(_SCRIPT.replace("__ID__", escapejs(str(field_id))))
        return mark_safe(f"{area}{table}{builder}{script}")
