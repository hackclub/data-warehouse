<script>
  let { schema = [], selectedTables = [], sensitiveColumns = {}, programName = 'database', isAirtable = false, formAction, backUrl = '', csrfToken, wid = '' } = $props()

  const qname = t => `${t.schema || 'public'}.${t.name}`
  const canon = name => {
    const t = schema.find(t => qname(t) === name) || schema.find(t => t.name === name)
    return t ? qname(t) : name
  }

  let search = $state('')
  let selected = $state(new Set(
    selectedTables.length > 0
      ? selectedTables.map(canon)
      : schema.filter(t => !t.excluded).map(qname)
  ))

  function initExcluded() {
    const manual = {}
    for (const [key, cols] of Object.entries(sensitiveColumns)) manual[canon(key)] = cols
    const result = {}
    for (const table of schema) {
      const auto = table.columns.filter(c => c.sensitive).map(c => c.name)
      const combined = [...new Set([...auto, ...(manual[qname(table)] || [])])]
      if (combined.length > 0) result[qname(table)] = combined
    }
    return result
  }
  let excluded = $state(initExcluded())

  let filtered = $derived(
    schema.filter(t => t.name.toLowerCase().includes(search.toLowerCase()))
  )

  let schemaGroups = $derived(
    Object.entries(
      filtered.reduce((acc, t) => {
        const s = t.schema || 'public'
        ;(acc[s] ||= []).push(t)
        return acc
      }, {})
    ).sort((a, b) => a[0].localeCompare(b[0]))
  )

  let multiSchema = $derived(schemaGroups.length > 1)

  function toggleTable(e, key, isExcluded) {
    e.preventDefault()
    if (isExcluded) return
    if (selected.has(key)) {
      selected.delete(key)
    } else {
      selected.add(key)
    }
    selected = new Set(selected)
  }

  function toggleColumn(key, col) {
    if (!excluded[key]) excluded[key] = []
    const idx = excluded[key].indexOf(col)
    if (idx >= 0) {
      excluded[key].splice(idx, 1)
    } else {
      excluded[key].push(col)
    }
    excluded = { ...excluded }
  }

  function isExcluded(key, col) {
    return (excluded[key] || []).includes(col)
  }

  function excludedCount(key) {
    return (excluded[key] || []).length
  }

  function resetExcluded(e, key) {
    e.stopPropagation()
    delete excluded[key]
    excluded = { ...excluded }
  }

  function selectAll() { selected = new Set(schema.filter(t => !t.excluded).map(qname)) }
  function selectNone() { selected = new Set() }

  function fmt(n) {
    if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`
    if (n >= 1000) return `${(n / 1000).toFixed(1)}K`
    return `${n}`
  }
</script>

<div class="schema-explorer">
  <div class="schema-header">
    <h2>Here's everything we'll sync</h2>
    <div class="schema-controls">
      <input type="text" bind:value={search} placeholder="filter tables..." class="filter-input" />
      <button type="button" onclick={selectAll} class="btn-sm">Sync all</button>
      <button type="button" onclick={selectNone} class="btn-sm">Clear</button>
    </div>
  </div>

  {#if isAirtable}
    <p class="tv-hint">everything syncs by default. the Airtable loader has no per-field allowlist, so a flagged field can't be dropped from the sync — deselect the whole table, or click the field to sync it deliberately.</p>
  {:else}
    <p class="tv-hint">everything syncs by default. columns whose <em>name</em> looks sensitive are pre-excluded — that's a guess off the name only, so check anything holding free text or json yourself. click any column to toggle.</p>
  {/if}

  {#snippet tableNode(table)}
    {@const key = qname(table)}
    {@const exc = excludedCount(key)}
    {@const isSel = selected.has(key)}
    <li class:tv-dim={table.excluded || !isSel}>
      {#if !table.excluded && isSel}
        <details open>
          <summary>
            <label class="tv-table-check">
              <input type="checkbox" checked={isSel} disabled={table.excluded}
                     onclick={(e) => { e.stopPropagation(); toggleTable(e, key, table.excluded) }} />
            </label>
            <span class="tv-table-name">{table.name}</span>
            <span class="tv-count">{fmt(table.row_count)}</span>
            {#if exc > 0}
              <button type="button" class="tv-badge tv-badge-warn tv-badge-reset" onclick={(e) => resetExcluded(e, key)}>{exc} col{exc > 1 ? 's' : ''} excluded — reset</button>
            {/if}
          </summary>
          <ul>
            {#each table.columns as col}
              {@const colExcl = isExcluded(key, col.name)}
              <li class="tv-col" class:tv-col-excluded={colExcl}>
                <button type="button" class="tv-col-btn" onclick={() => toggleColumn(key, col.name)}>
                  <span class="tv-col-name" class:tv-strike={colExcl}>{col.name}</span>
                  <span class="tv-col-type">{col.udt || col.type}</span>
                  {#if table.primary_key.includes(col.name)}<span class="tv-col-pk">PK</span>{/if}
                  {#if colExcl}<span class="tv-col-tag tv-tag-excl">{isAirtable ? 'blocks the sync' : 'excluded'}</span>
                  {:else if col.sensitive}<span class="tv-col-tag tv-tag-sens" title="Name contains token/secret/password/etc — click to include anyway">{isAirtable ? 'sensitive — blocks the sync' : 'excluded (sensitive)'}</span>{/if}
                </button>
              </li>
            {/each}
          </ul>
        </details>
      {:else}
        <label class="tv-table-check">
          <input type="checkbox" checked={isSel} disabled={table.excluded}
                 onclick={(e) => toggleTable(e, key, table.excluded)} />
        </label>
        <span class="tv-table-name">{table.name}</span>
        <span class="tv-count">{fmt(table.row_count)}</span>
        {#if table.excluded}<span class="tv-badge tv-badge-skip" title="Migration/queue tables are excluded by default">auto-excluded</span>{/if}
      {/if}
    </li>
  {/snippet}

  <ul class="tree-view">
    <li>
      <span class="tv-root">{programName}</span>
      <ul>
        {#each schemaGroups as [schemaName, tables]}
          {#if multiSchema}
            <li>
              <details open>
                <summary><span class="tv-schema">{schemaName}</span></summary>
                <ul>
                  {#each tables as table (qname(table))}
                    {@render tableNode(table)}
                  {/each}
                </ul>
              </details>
            </li>
          {:else}
            {#each tables as table (qname(table))}
              {@render tableNode(table)}
            {/each}
          {/if}
        {/each}
      </ul>
    </li>
  </ul>

  <form action={formAction} method="post">
    <input type="hidden" name="authenticity_token" value={csrfToken} />
    <input type="hidden" name="_method" value="patch" />
    {#if wid}<input type="hidden" name="wid" value={wid} />{/if}
    {#each [...selected] as table}
      <input type="hidden" name="tables[]" value={table} />
    {/each}
    {#each Object.entries(excluded) as [table, cols]}
      {#each cols as col}
        <input type="hidden" name={`sensitive_columns[${table}][]`} value={col} />
      {/each}
    {/each}
    <div class="schema-footer">
      {#if backUrl}
        <a href={backUrl} class="btn-back">← Back</a>
      {/if}
      <span class="schema-summary">{selected.size} of {schema.length} tables selected</span>
      <button type="submit" class="btn-chrome" disabled={selected.size === 0}>Continue →</button>
    </div>
  </form>
</div>

<style>
  .schema-explorer { width: 100%; }

  .schema-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    flex-wrap: wrap;
    gap: 12px;
  }
  .schema-header h2 {
    color: var(--gold-bright, #e0c840);
    font-size: 20px;
    margin: 0;
  }
  .schema-controls { display: flex; gap: 8px; align-items: center; }

  .filter-input {
    background: var(--input-bg, #e8e4d8);
    border: 2px solid var(--gold, #c8a020);
    color: #111;
    font-family: inherit;
    font-size: 15px;
    padding: 6px 12px;
  }
  .filter-input:focus { outline: none; box-shadow: 0 0 6px rgba(200, 160, 32, 0.4); }

  .btn-sm {
    background: transparent;
    border: 1px solid var(--gold-dim, #8a6c18);
    color: var(--chrome-text, #d8d0c0);
    font-family: inherit;
    font-size: 16px;
    padding: 4px 12px;
    cursor: pointer;
  }
  .btn-sm:hover { color: var(--gold-bright, #e0c840); border-color: var(--gold, #c8a020); }

  .tv-hint {
    color: #555577;
    font-size: 10px;
    font-family: Verdana, Geneva, Arial, sans-serif;
    margin: 0 0 6px 0;
    font-style: italic;
  }

  /* === 98.css treeview, dark-adapted === */
  ul.tree-view {
    --tv-bg: #131322;
    --tv-line: #555577;
    background: var(--tv-bg);
    box-shadow: inset -1px -1px #2a2a44, inset 1px 1px #0a0a14, inset -2px -2px #1e1e36, inset 2px 2px #060610;
    display: block;
    margin: 0;
    padding: 8px;
    font-family: Verdana, Geneva, Arial, sans-serif;
    font-size: 11px;
    line-height: 1.6;
    max-height: 520px;
    overflow: auto;
  }

  ul.tree-view li { list-style-type: none; }

  ul.tree-view li,
  ul.tree-view ul { margin-top: 3px; }

  ul.tree-view ul {
    border-left: 1px dotted var(--tv-line);
    margin-left: 16px;
    padding-left: 16px;
  }

  ul.tree-view ul > li { position: relative; }

  ul.tree-view ul > li::before {
    border-bottom: 1px dotted var(--tv-line);
    content: "";
    display: block;
    left: -16px;
    position: absolute;
    top: 8px;
    width: 12px;
  }

  ul.tree-view ul > li:last-child::after {
    background: var(--tv-bg);
    bottom: 0;
    content: "";
    display: block;
    left: -20px;
    position: absolute;
    top: 9px;
    width: 8px;
  }

  /* details/summary expand-collapse */
  ul.tree-view details { margin-top: 0; }
  ul.tree-view details[open] summary { margin-bottom: 0; }

  ul.tree-view ul details > summary::before {
    margin-left: -22px;
    position: relative;
    z-index: 1;
  }

  ul.tree-view details > summary::before {
    background-color: var(--tv-bg);
    border: 1px solid var(--tv-line);
    color: var(--chrome-text, #d8d0c0);
    content: "+";
    display: block;
    float: left;
    height: 9px;
    line-height: 8px;
    margin-right: 5px;
    padding-left: 1px;
    text-align: center;
    width: 8px;
    font-size: 10px;
  }

  ul.tree-view details[open] > summary::before { content: "−"; }

  ul.tree-view details > summary::-webkit-details-marker,
  ul.tree-view details > summary::marker { content: ""; }

  ul.tree-view details > summary {
    cursor: pointer;
    user-select: none;
    list-style: none;
  }

  .tv-root {
    color: var(--gold-bright, #e0c840);
    font-weight: bold;
    font-size: 12px;
  }
  .tv-schema {
    color: var(--gold, #c8a020);
    font-weight: bold;
  }

  .tv-dim { opacity: 0.4; }

  .tv-table-check {
    cursor: pointer;
    display: inline;
    margin-right: 3px;
    vertical-align: middle;
  }
  .tv-table-check input {
    margin: 0;
    cursor: pointer;
    vertical-align: middle;
  }

  .tv-table-name {
    color: var(--gold, #c8a020);
    font-weight: bold;
  }
  .tv-count {
    color: #666688;
    margin-left: 6px;
    font-size: 10px;
  }

  .tv-badge {
    font-size: 9px;
    margin-left: 6px;
    padding: 1px 5px;
    border-radius: 2px;
    vertical-align: middle;
  }
  .tv-badge-skip { background: rgba(100, 100, 130, 0.3); color: #888899; }
  .tv-badge-warn { background: rgba(255, 68, 136, 0.15); color: #ff6699; }
  .tv-badge-reset { cursor: pointer; }
  .tv-badge-reset:hover { background: rgba(255, 68, 136, 0.3); color: #ff88aa; }

  /* Column rows */
  .tv-col {
    padding: 0;
    border-radius: 2px;
  }
  .tv-col-excluded { opacity: 0.5; }

  .tv-col-btn {
    all: unset;
    display: block;
    width: 100%;
    cursor: pointer;
    padding: 1px 4px;
    border-radius: 2px;
    box-sizing: border-box;
    text-align: left;
  }
  .tv-col-btn:hover { background: rgba(200, 160, 32, 0.08); }
  .tv-col-btn:focus-visible { outline: 2px solid var(--gold, #c8a020); outline-offset: -1px; }

  .tv-col-name { color: var(--chrome-text, #d8d0c0); }
  .tv-strike { text-decoration: line-through; color: #666688; }
  .tv-col-type { color: #556688; margin-left: 4px; font-size: 10px; }
  .tv-col-pk {
    color: var(--gold-dim, #8a6c18);
    margin-left: 4px;
    font-size: 8px;
    font-weight: bold;
    border: 1px solid var(--gold-dim, #8a6c18);
    padding: 0 2px;
    border-radius: 1px;
    vertical-align: middle;
  }

  .tv-col-tag {
    margin-left: 4px;
    font-size: 8px;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }
  .tv-tag-excl { color: #cc4466; }
  .tv-tag-sens { color: #cc4466; }

  .schema-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 16px;
    padding-top: 12px;
    border-top: 1px solid var(--chrome-border, #3a3a55);
    gap: 16px;
  }
  .schema-summary { color: var(--gold-dim, #8a6c18); font-size: 14px; flex: 1; text-align: center; }
</style>
