<script>
  let { schema = [], selectedTables = [], syncConfigs = {}, formAction, backUrl = '', csrfToken, wid = '', targetSchema = '' } = $props()

  const qname = t => `${t.schema || 'public'}.${t.name}`
  const canon = name => {
    const t = schema.find(t => qname(t) === name) || schema.find(t => t.name === name)
    return t ? qname(t) : name
  }

  let configs = $state(Object.fromEntries(Object.entries(syncConfigs).map(([k, c]) => [canon(k), c])))
  let defaultMode = $state(_guessDefault(syncConfigs))
  let overrideSchema = $state(targetSchema)

  function _guessDefault(cfgs) {
    const modes = Object.values(cfgs).map(c => c.mode).filter(Boolean)
    if (!modes.length) return 'full-refresh'
    const inc = modes.filter(m => m === 'incremental').length
    return inc > modes.length / 2 ? 'incremental' : 'full-refresh'
  }

  let selectedKeys = $derived(new Set(selectedTables.map(canon)))
  let tables = $derived(schema.filter(t => selectedKeys.has(qname(t))))

  let effective = $derived(
    Object.fromEntries(tables.map(t => [qname(t), configs[qname(t)] || suggestConfig(t)]))
  )

  let multiSchema = $derived(new Set(tables.map(t => t.schema || 'public')).size > 1)

  function defaultPrimaryKey(table) {
    if (table.primary_key?.length) return table.primary_key
    const cols = allColumns(table)
    return cols.includes('id') ? ['id'] : cols.slice(0, 1)
  }

  function incrementalConfig(table) {
    return {
      mode: 'incremental',
      primary_key: defaultPrimaryKey(table),
      update_key: table.has_updated_at
        ? 'updated_at'
        : table.has_created_at
          ? 'created_at'
          : timestampColumns(table)[0] || allColumns(table)[0],
    }
  }

  function suggestConfig(table) {
    if (defaultMode === 'incremental' && table.has_updated_at) return incrementalConfig(table)
    if (table.row_count < 50000) {
      return defaultMode === 'incremental' ? incrementalConfig(table) : { mode: 'full-refresh' }
    }
    if (table.has_updated_at || table.has_created_at) return incrementalConfig(table)
    return { mode: 'full-refresh' }
  }

  function setMode(table, mode) {
    configs[qname(table)] = mode === 'incremental' ? incrementalConfig(table) : { mode: 'full-refresh' }
    configs = { ...configs }
  }

  function setUpdateKey(table, col) {
    configs[qname(table)] = { ...effective[qname(table)], update_key: col }
    configs = { ...configs }
  }

  function setPrimaryKey(table, cols) {
    configs[qname(table)] = { ...effective[qname(table)], primary_key: cols }
    configs = { ...configs }
  }

  function togglePkCol(table, col) {
    const current = effective[qname(table)].primary_key || table.primary_key || []
    const idx = current.indexOf(col)
    const next = idx >= 0 ? current.filter(c => c !== col) : [...current, col]
    if (next.length > 0) setPrimaryKey(table, next)
  }

  function timestampColumns(table) {
    return table.columns
      .filter(c => ['timestamp', 'timestamptz', 'timestamp with time zone', 'timestamp without time zone', 'date'].includes(c.udt || c.type))
      .map(c => c.name)
  }

  function allColumns(table) {
    return table.columns.map(c => c.name)
  }

  function formatCount(n) {
    if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M rows`
    if (n >= 1000) return `${(n / 1000).toFixed(1)}K rows`
    return `${n} rows`
  }

  let incCount = $derived(Object.values(effective).filter(c => c.mode === 'incremental').length)
  let fullCount = $derived(tables.length - incCount)
</script>

<div class="sync-config">
  <h2>Sync Configuration</h2>
  <p>How should we keep tables in sync? Full Refresh is safest — it replaces all data each run.</p>

  <div class="sync-global">
    <div class="sync-default-mode">
      <span class="sync-label">Default mode:</span>
      <label class="sync-radio">
        <input type="radio" name="default_mode" checked={defaultMode === 'full-refresh'}
               onchange={() => defaultMode = 'full-refresh'} />
        Full Refresh <span class="sync-recommended">(recommended)</span>
      </label>
      <label class="sync-radio">
        <input type="radio" name="default_mode" checked={defaultMode === 'incremental'}
               onchange={() => defaultMode = 'incremental'} />
        Incremental
      </label>
      <span class="sync-mode-summary">{incCount} incremental, {fullCount} full-refresh</span>
    </div>
    <p class="sync-hint">If you don't know what this means, pick Full Refresh. Incremental only syncs new/changed rows and requires a timestamp column on every table — misconfigured, it silently drops data.</p>

    {#if overrideSchema}
      <div class="sync-schema-note">
        Target schema: <strong>{overrideSchema}</strong> <span class="sync-dim">(overridden from "{schema[0]?.name || 'program_name'}")</span>
      </div>
    {/if}
  </div>

  <div class="sync-tables">
    {#each tables as table (qname(table))}
      {@const cfg = effective[qname(table)]}
      <div class="sync-card">
        <div class="sync-card-header">
          <span class="sync-table-name">{multiSchema ? qname(table) : table.name}</span>
          <span class="sync-table-count">{formatCount(table.row_count)}</span>
        </div>
        <div class="sync-card-body">
          <div class="sync-mode">
            <label class="sync-radio">
              <input type="radio" name={`mode_${qname(table)}`}
                     checked={cfg.mode === 'full-refresh'}
                     onchange={() => setMode(table, 'full-refresh')} />
              Full Refresh
            </label>
            <label class="sync-radio">
              <input type="radio" name={`mode_${qname(table)}`}
                     checked={cfg.mode === 'incremental'}
                     onchange={() => setMode(table, 'incremental')} />
              Incremental
            </label>
          </div>
          {#if cfg.mode === 'incremental'}
            <div class="sync-incremental-opts">
              <label>
                Update key:
                <select onchange={(e) => setUpdateKey(table, e.target.value)} class="terminal-select">
                  {#each timestampColumns(table) as col}
                    <option value={col} selected={cfg.update_key === col}>{col}</option>
                  {/each}
                  {#each allColumns(table).filter(c => !timestampColumns(table).includes(c)) as col}
                    <option value={col} selected={cfg.update_key === col}>{col}</option>
                  {/each}
                </select>
              </label>
              <div class="sync-pk-picker">
                <span class="sync-pk-label">PK:</span>
                {#each allColumns(table) as col}
                  {@const active = (cfg.primary_key || table.primary_key || []).includes(col)}
                  <button type="button" class="sync-pk-chip" class:sync-pk-active={active}
                          onclick={() => togglePkCol(table, col)}>{col}</button>
                {/each}
              </div>
            </div>
          {/if}
        </div>
      </div>
    {/each}
  </div>

  <form action={formAction} method="post">
    <input type="hidden" name="authenticity_token" value={csrfToken} />
    <input type="hidden" name="_method" value="patch" />
    {#if wid}<input type="hidden" name="wid" value={wid} />{/if}
    <input type="hidden" name="sync_configs" value={JSON.stringify(effective)} />
    {#if overrideSchema}<input type="hidden" name="target_schema" value={overrideSchema} />{/if}
    <div class="sync-submit">
      {#if backUrl}
        <a href={backUrl} class="btn-back">← Back</a>
      {/if}
      <button type="submit" class="btn-chrome">Continue →</button>
    </div>
  </form>
</div>

<style>
  .sync-config h2 {
    margin: 0 0 8px;
    font-size: 18px;
    color: var(--gold-bright, #e0c840);
  }

  .sync-global {
    margin: 16px 0;
    padding: 12px 16px;
    border: 1px solid var(--chrome-border, #3a3a55);
    background: rgba(0,0,0,0.15);
  }

  .sync-default-mode {
    display: flex;
    align-items: center;
    gap: 16px;
    font-size: 14px;
  }
  .sync-label { color: var(--gold, #c8a020); font-weight: bold; }
  .sync-mode-summary { color: #666688; font-size: 12px; margin-left: auto; }
  .sync-recommended { color: #44cc88; font-size: 11px; }
  .sync-hint {
    margin: 8px 0 0;
    font-size: 11px;
    color: #666688;
    line-height: 1.4;
  }

  .sync-schema-note {
    margin-top: 8px;
    font-size: 12px;
    color: var(--gold-dim, #8a6c18);
  }
  .sync-dim { color: #555577; }

  .sync-tables {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin: 20px 0;
  }

  .sync-card {
    border: 2px solid var(--chrome-border, #3a3a55);
    background: rgba(0,0,0,0.2);
  }

  .sync-card-header {
    display: flex;
    justify-content: space-between;
    padding: 10px 16px;
    background: rgba(0,0,0,0.2);
    border-bottom: 1px solid rgba(255,255,255,0.05);
  }
  .sync-table-name { font-size: 15px; font-weight: bold; color: var(--gold, #c8a020); }
  .sync-table-count { color: #888899; font-size: 13px; }

  .sync-card-body { padding: 12px 16px; }

  .sync-mode {
    display: flex;
    gap: 24px;
    font-size: 14px;
  }
  .sync-radio { cursor: pointer; display: flex; align-items: center; gap: 6px; }

  .sync-incremental-opts {
    margin-top: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    font-size: 14px;
  }

  .terminal-select {
    background: #1a1a2e;
    border: 2px solid var(--gold, #c8a020);
    color: var(--content-text, #d0c8b8);
    font-family: inherit;
    font-size: 14px;
    padding: 4px 8px;
  }

  .sync-pk-picker {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    align-items: center;
  }
  .sync-pk-label { color: #888899; font-size: 12px; margin-right: 4px; }
  .sync-pk-chip {
    background: transparent;
    border: 1px solid #3a3a55;
    color: #666688;
    font-family: inherit;
    font-size: 11px;
    padding: 2px 8px;
    cursor: pointer;
  }
  .sync-pk-chip:hover { border-color: var(--gold-dim, #8a6c18); color: #888899; }
  .sync-pk-active {
    background: rgba(200, 160, 32, 0.15);
    border-color: var(--gold, #c8a020);
    color: var(--gold, #c8a020);
  }

  .sync-submit {
    display: flex;
    justify-content: space-between;
    margin-top: 16px;
  }
</style>
