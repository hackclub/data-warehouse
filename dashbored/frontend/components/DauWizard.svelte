<script>
  let { schema = [], selectedTables = [], programName = '', dauConfig = {}, formAction, backUrl = '', csrfToken, wid = '', testSqlUrl = '' } = $props()

  // Table keys are schema-qualified everywhere selection state is, and both
  // generators resolve "schema.table" (CodeGenerator._table_info) — so a config
  // saved before that, or by an older wizard, gets canonicalised on the way in
  // or its select renders blank while the bare name posts.
  const qname = t => `${t.schema || 'public'}.${t.name}`
  const canon = name => {
    const t = schema.find(t => qname(t) === name) || schema.find(t => t.name === name)
    return t ? qname(t) : name
  }

  let usesHackatime = $state(dauConfig.uses_hackatime || false)
  let hasCustomTime = $state(dauConfig.has_custom_time || false)
  let useCustomSql = $state(dauConfig.use_custom_sql || false)
  let customSql = $state(dauConfig.custom_sql || '')
  let promptCopied = $state(false)
  let testLoading = $state(false)
  let testResults = $state(null)
  let testError = $state(null)
  let htMappingTable = $state(canon(dauConfig.ht_mapping_table || ''))
  let htUserColumn = $state(dauConfig.ht_user_column || '')
  let htAliasColumn = $state(dauConfig.ht_alias_column || '')
  let htAliasFormat = $state(dauConfig.ht_alias_format || 'single')
  let claimStartSource = $state(dauConfig.claim_start_source || 'column')
  let claimStartColumn = $state(dauConfig.claim_start_column || '')
  let claimStartDate = $state(dauConfig.claim_start_date || '')
  let htEmailTable = $state(canon(dauConfig.ht_email_table || ''))
  let htEmailColumn = $state(dauConfig.ht_email_column || '')
  let customTable = $state(canon(dauConfig.custom_table || ''))
  let timestampColumn = $state(dauConfig.timestamp_column || '')
  let userColumn = $state(dauConfig.user_column || '')
  let durationColumn = $state(dauConfig.duration_column || '')
  let emailTable = $state(canon(dauConfig.email_table || ''))
  let emailColumn = $state(dauConfig.email_column || '')
  let durationUnit = $state(dauConfig.duration_unit || 'seconds')
  let cap24h = $state(dauConfig.cap_24h || false)
  let startDate = $state(dauConfig.start_date || '')
  let endDate = $state(dauConfig.end_date || '')

  let selectedKeys = $derived(new Set(selectedTables.map(canon)))
  let tables = $derived(schema.filter(t => selectedKeys.has(qname(t))))
  let isAirtable = $derived(schema.some(t => t.schema === 'airtable'))

  // Mirrors airtable_code_generator.sanitize_name — how a display name is spelled
  // once dlt has landed it in the warehouse. Sling keeps names as they are.
  function sanitizeName(name) {
    let s = String(name).replace(/^(?=\p{Nd})/u, '_').replace(/[^\p{L}\p{N}_]/gu, '_')
    s = s.replace(/[A-Z]+/g, m => `_${m}`).toLowerCase().replace(/^_+|_+$/g, '')
    s = s.replace(/_+/g, '_')
    return /^\p{Nd}/u.test(s) ? `_${s}` : s
  }

  const warehouseName = name => (isAirtable ? sanitizeName(name) : name)
  let sourceName = $derived(isAirtable ? `airtable_${programName}` : programName)

  // Sling writes every stream to one target schema, so CodeGenerator._warehouse_names
  // qualifies a table name two selected schemas both claim.
  let warehouseTable = $derived.by(() => {
    if (isAirtable) return t => sanitizeName(t.name)
    const seen = tables.map(t => t.name)
    return t => (seen.indexOf(t.name) === seen.lastIndexOf(t.name) ? t.name : `${t.schema || 'public'}_${t.name}`)
  })

  // Options always carry the qualified key; the label only spells the schema out
  // when that is what tells two selected tables apart.
  let tableLabel = $derived.by(() => {
    const seen = tables.map(t => t.name)
    return t => (seen.indexOf(t.name) === seen.lastIndexOf(t.name) ? t.name : qname(t))
  })

  // The email table defaults to `users` only when exactly one selected table is
  // spelled that way — with public.users and analytics.users both in, picking
  // one silently changes the join, so make the user say which.
  function defaultEmailTable() {
    const matches = tables.filter(t => t.name === 'users')
    return matches.length === 1 ? qname(matches[0]) : ''
  }

  let activityTables = $derived(
    tables.filter(t => {
      const name = t.name.toLowerCase()
      return ['activit', 'session', 'event', 'log', 'entr', 'journal', 'devlog', 'heartbeat', 'work', 'time'].some(
        kw => name.includes(kw)
      )
    })
  )

  function columnsForTable(tableName) {
    const t = schema.find(s => s.name === tableName || qname(s) === tableName)
    return t ? t.columns : []
  }

  function userColumns(tableName) {
    return columnsForTable(tableName).filter(c => {
      const name = c.name.toLowerCase()
      return ['email', 'user_email', 'user_id', 'slack_id', 'author_id', 'creator_id', 'owner_id'].includes(name)
        || name.includes('email') || name.includes('user')
    })
  }

  function durationColumns(tableName) {
    return columnsForTable(tableName).filter(c => {
      const name = c.name.toLowerCase()
      const udt = (c.udt || c.type || '').toLowerCase()
      return ['duration', 'seconds', 'minutes', 'hours', 'time_spent', 'elapsed', 'length'].some(kw => name.includes(kw))
        || ['integer', 'int4', 'int8', 'bigint', 'float4', 'float8', 'numeric', 'real', 'double precision', 'number', 'currency', 'percent'].includes(udt)
    })
  }

  function aliasColumns(tableName) {
    return columnsForTable(tableName).filter(c => {
      const name = c.name.toLowerCase()
      return ['name', 'alias', 'project', 'hackatime', 'project_name', 'repo', 'slug'].some(kw => name.includes(kw))
    })
  }

  function timestampColumns(tableName) {
    return columnsForTable(tableName).filter(c => {
      const udt = (c.udt || c.type || '').toLowerCase()
      const name = c.name.toLowerCase()
      return ['timestamp', 'timestamptz', 'datetime', 'date', 'createdtime', 'lastmodifiedtime'].includes(udt)
        || ['created_at', 'inserted_at', 'created', 'timestamp', 'date', 'started_at', 'created time', 'last modified', 'last modified time'].includes(name)
    })
  }

  function autoPick(candidates, current) {
    if (current) return current
    return candidates.length === 1 ? candidates[0].name : ''
  }

  // Mirrors DauSource.is_id_column in app/services/dau_sql.py: both the raw
  // spelling and the warehouse one count, so Airtable's "User ID" is an id here
  // too and the email-table picker actually appears.
  function looksLikeId(colName, tableName) {
    if (!colName) return false
    for (const spelling of [colName, warehouseName(colName)]) {
      if (spelling === 'id' || spelling.endsWith('_id')) return true
    }
    const col = columnsForTable(tableName).find(c => c.name === colName)
    if (!col) return false
    return ['int4', 'int8', 'integer', 'bigint', 'serial'].includes((col.udt || col.type || '').toLowerCase())
  }

  function emailColumns(tableName) {
    return columnsForTable(tableName).filter(c => {
      const name = c.name.toLowerCase()
      return name.includes('email') || name.includes('mail')
    })
  }

  let htUserIsId = $derived(looksLikeId(htUserColumn, htMappingTable))
  let customUserIsId = $derived(looksLikeId(userColumn, customTable))

  // Qualified spellings are legal server-side, so never prune one — repointing
  // analytics.users at public.users would silently change the join.
  const isSelected = name => tables.some(t => t.name === name || qname(t) === name)

  // A table dropped on the way back through table selection is still in
  // dauConfig, and the select renders blank while the stale name posts. Only
  // selected tables are in sources.yml, so keeping one breaks the dbt compile.
  $effect(() => {
    if (htMappingTable && !isSelected(htMappingTable)) {
      htMappingTable = ''
      htUserColumn = ''
      htAliasColumn = ''
      claimStartColumn = ''
    }
    if (customTable && !isSelected(customTable)) {
      customTable = ''
      userColumn = ''
      durationColumn = ''
      timestampColumn = ''
    }
  })

  $effect(() => {
    if (!htMappingTable) return
    htUserColumn = autoPick(userColumns(htMappingTable), htUserColumn)
    htAliasColumn = autoPick(aliasColumns(htMappingTable), htAliasColumn)
    const ts = timestampColumns(htMappingTable)
    const created = columnsForTable(htMappingTable).find(c => c.name === 'created_at')
    claimStartColumn = claimStartColumn || (created ? 'created_at' : ts.length === 1 ? ts[0].name : '')
  })

  // The column has to belong to the table currently picked — clearing or
  // switching the email table must not leave the old column behind.
  $effect(() => {
    if (!htUserIsId) { htEmailTable = ''; htEmailColumn = ''; return }
    if (htEmailTable && !isSelected(htEmailTable)) htEmailTable = ''
    if (!htEmailTable) htEmailTable = defaultEmailTable()
    if (!columnsForTable(htEmailTable).some(c => c.name === htEmailColumn)) {
      htEmailColumn = autoPick(emailColumns(htEmailTable), '')
    }
  })

  $effect(() => {
    if (!customUserIsId) { emailTable = ''; emailColumn = ''; return }
    if (emailTable && !isSelected(emailTable)) emailTable = ''
    if (!emailTable) emailTable = defaultEmailTable()
    if (!columnsForTable(emailTable).some(c => c.name === emailColumn)) {
      emailColumn = autoPick(emailColumns(emailTable), '')
    }
  })

  $effect(() => {
    if (!customTable) return
    userColumn = autoPick(userColumns(customTable), userColumn)
    const durCols = columnsForTable(customTable).filter(c =>
      ['duration', 'seconds', 'minutes', 'hours', 'time_spent', 'elapsed'].some(kw => c.name.includes(kw))
    )
    durationColumn = autoPick(durCols, durationColumn)
    const created = columnsForTable(customTable).find(c => c.name === 'created_at')
    const ts = timestampColumns(customTable)
    timestampColumn = timestampColumn || (created ? 'created_at' : ts.length === 1 ? ts[0].name : '')
  })

  function missingFields() {
    // An empty box is not an override: CodeGenerator.generate only takes the
    // custom-SQL branch for non-blank SQL, so ticking the box and leaving it
    // blank falls back to the auto path and has to be validated as one.
    if (!usesHackatime && !hasCustomTime && !(useCustomSql && customSql.trim())) {
      return ['Select at least one time tracking method or write custom SQL.']
    }
    // Custom SQL replaces both generated CTEs, so nothing that only feeds them
    // is required — the generator never reads those fields on this branch.
    if (useCustomSql && customSql.trim()) return []
    const out = []
    if (usesHackatime) {
      if (!htMappingTable) out.push('Pick the table where users claim their Hackatime alias.')
      else {
        if (!htUserColumn) out.push('Pick the user column on the Hackatime mapping table.')
        if (htUserIsId && (!htEmailTable || !htEmailColumn)) out.push('Pick the table and column holding user emails.')
        if (!htAliasColumn) out.push('Pick the Hackatime alias column.')
        if (claimStartSource === 'column') {
          if (!claimStartColumn) out.push('Pick the column marking when each alias was claimed.')
        } else if (!claimStartDate) {
          out.push('Pick the date all aliases count from.')
        }
      }
    }
    if (hasCustomTime) {
      if (!customTable) out.push('Pick the custom activity table.')
      else {
        if (!userColumn) out.push('Pick the user column on the activity table.')
        if (customUserIsId && (!emailTable || !emailColumn)) out.push('Pick the table and column holding user emails.')
        if (!timestampColumn) out.push('Pick the timestamp column on the activity table.')
      }
    }
    return out
  }

  let missing = $derived(missingFields())

  function generatePrompt() {
    const sensitiveNote = isAirtable
      ? ' [SENSITIVE - synced anyway, do not select]'
      : ' [SENSITIVE - excluded from sync]'
    const tableList = tables.map(t => {
      const cols = t.columns.map(c => {
        const name = warehouseName(c.name)
        const spelled = /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`
        const origin = name === c.name ? '' : ` — Airtable field "${c.name}"`
        return `    - ${spelled} (${c.udt || c.type})${origin}${c.sensitive ? sensitiveNote : ''}`
      }).join('\n')
      const rows = t.row_count ? `  (${t.row_count} rows)` : ''
      return `  {{ source('${sourceName}', '${warehouseTable(t)}') }}${rows}\n${cols}`
    }).join('\n\n')

    let needs = ''
    if (usesHackatime) {
      needs += `
### Hackatime Claims CTE
Write a CTE called \`${programName}_ht_claims\` that maps users to their Hackatime project aliases.
Must produce EXACTLY these columns in this order:
1. program_name (text) — literal '${programName}'::text
2. user_email (text) — normalized email (see email normalization pattern below)
3. hackatime_alias (text) — LOWER(BTRIM(alias_column))
4. project_name (text) — project title, or NULL::text
5. code_url (text) — repo/code URL, or NULL::text
6. claim_start_ts (timestamptz) — when this claim started (usually created_at AT TIME ZONE 'UTC')

Filter out rows where the alias is NULL or empty.
`
    }
    if (hasCustomTime) {
      needs += `
### Custom Hourly CTE
Write a CTE called \`${programName}_custom_hourly\` that aggregates activity/time data into hourly buckets.
Must produce EXACTLY these columns in this order:
1. activity_hour (timestamptz) — DATE_TRUNC('hour', timestamp_col AT TIME ZONE 'UTC')
2. program_name (text) — literal '${programName}'::text
3. user_email (text) — normalized email (see below)
4. project_name (text) — project title, or NULL::text
5. code_url (text) — repo/code URL, or NULL::text
6. raw_hours_logged (numeric) — ROUND(SUM(duration_in_hours)::numeric, 4)
7. logging_method (text) — literal 'custom'::text
8. source_detail (text) — provenance string like '${programName}.table_name.duration_col; entries=' || COUNT(*)::text

GROUP BY columns 1-5. Filter out zero-duration entries.
`
    }

    return `I need SQL CTEs for a program called "${programName}" in a data warehouse DAU (Daily Active Users) pipeline.

The program's data is synced into a warehouse schema called "${sourceName}". Reference tables using dbt's source() macro.
Table and column names below are the warehouse spellings — use them exactly as written.

## Available tables:
${tableList}

## What I need:
${needs}
## Email normalization pattern (use this for all user_email columns):
CASE WHEN POSITION('@' IN LOWER(BTRIM(email_col))) > 0
     THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email_col)), '@', 1), '+', 1)
          || '@' || SPLIT_PART(LOWER(BTRIM(email_col)), '@', 2)
     ELSE SPLIT_PART(LOWER(BTRIM(email_col)), '+', 1)
END

If the user identifier column is an integer FK (like user_id), JOIN to the users table and normalize users.email instead.

## Rules:
- Reference tables as {{ source('${sourceName}', 'table_name') }}
- End each CTE with a trailing comma
- Output ONLY the CTE definitions (WITH is not needed — these slot into an existing WITH block)
- Do not wrap in \`\`\` code fences`
  }

  async function copyPrompt() {
    await navigator.clipboard.writeText(generatePrompt())
    promptCopied = true
    setTimeout(() => promptCopied = false, 2000)
  }

  async function testSql() {
    testLoading = true
    testResults = null
    testError = null
    try {
      const res = await fetch(testSqlUrl || '/api/test_sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: customSql, wid }),
      })
      const data = await res.json()
      if (data.error) {
        testError = data.error
      } else {
        testResults = data
      }
    } catch (e) {
      testError = e.message || 'Request failed'
    } finally {
      testLoading = false
    }
  }

  function buildConfig() {
    return JSON.stringify({
      uses_hackatime: usesHackatime,
      has_custom_time: hasCustomTime,
      use_custom_sql: useCustomSql,
      custom_sql: useCustomSql ? customSql : '',
      ht_mapping_table: htMappingTable,
      ht_user_column: htUserColumn,
      ht_alias_column: htAliasColumn,
      ht_alias_format: htAliasFormat,
      claim_start_source: claimStartSource,
      claim_start_column: claimStartColumn,
      claim_start_date: claimStartDate,
      ht_email_table: htEmailTable,
      ht_email_column: htEmailColumn,
      custom_table: customTable,
      email_table: emailTable,
      email_column: emailColumn,
      timestamp_column: timestampColumn,
      user_column: userColumn,
      duration_column: durationColumn,
      duration_unit: durationUnit,
      cap_24h: cap24h,
      start_date: startDate,
      end_date: endDate,
    })
  }
</script>

<div class="dau-wizard">
  <h2>DAU Configuration</h2>
  <p>DAU calculation for <strong>{programName}</strong>.</p>

    <div class="dau-section">
      <h3>Program Window</h3>
      <div class="dau-dates">
        <label>
          Start date
          <input type="date" bind:value={startDate} class="webtv-input" />
        </label>
        <label>
          End date (leave blank if ongoing)
          <input type="date" bind:value={endDate} class="webtv-input" />
        </label>
      </div>
    </div>

    <div class="dau-section">
      <h3>Time Tracking Method</h3>
      <label class="dau-check">
        <input type="checkbox" bind:checked={usesHackatime} />
        Uses Hackatime for code time tracking
      </label>
      <label class="dau-check">
        <input type="checkbox" bind:checked={hasCustomTime} />
        Has custom time/activity logging (devlogs, journals, work sessions)
      </label>
    </div>

    {#if usesHackatime}
      <div class="dau-section">
        <h3>Hackatime Mapping</h3>
        <p class="dau-hint">Hackatime tracks coding time by "project alias" — a name each user sets in their editor. To count DAU, we need to know which aliases belong to your program. Pick the table where users register or claim their project.</p>
        <select bind:value={htMappingTable} class="webtv-input">
          <option value="">Select a table...</option>
          {#each tables as t}
            <option value={qname(t)}>{tableLabel(t)}</option>
          {/each}
        </select>

        {#if htMappingTable}
          <div class="dau-column-pickers">
            <label>
              Who's the user?
              <span class="dau-field-hint">The column that identifies each person (email, slack ID, etc.)</span>
              <select bind:value={htUserColumn} class="webtv-input">
                <option value="">Select...</option>
                {#each userColumns(htMappingTable) as c}
                  <option value={c.name}>{c.name}</option>
                {/each}
                <optgroup label="All columns">
                  {#each columnsForTable(htMappingTable) as c}
                    <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                  {/each}
                </optgroup>
              </select>
            </label>

            {#if htUserIsId}
              <label>
                Which table has user emails?
                <span class="dau-field-hint">The user column looks like a foreign key. We need to join against the table that has the email address.</span>
                <select bind:value={htEmailTable} class="webtv-input">
                  <option value="">Select...</option>
                  {#each tables as t}
                    <option value={qname(t)}>{tableLabel(t)}</option>
                  {/each}
                </select>
              </label>
              {#if htEmailTable}
                <label>
                  Email column
                  <select bind:value={htEmailColumn} class="webtv-input">
                    <option value="">Select...</option>
                    {#each emailColumns(htEmailTable) as c}
                      <option value={c.name}>{c.name}</option>
                    {/each}
                    <optgroup label="All columns">
                      {#each columnsForTable(htEmailTable) as c}
                        <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                      {/each}
                    </optgroup>
                  </select>
                </label>
              {/if}
            {/if}

            <label>
              What's their Hackatime alias?
              <span class="dau-field-hint">The project name they set in their editor — we'll match this against Hackatime heartbeats</span>
              <select bind:value={htAliasColumn} class="webtv-input">
                <option value="">Select...</option>
                {#each aliasColumns(htMappingTable) as c}
                  <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                {/each}
                <optgroup label="All columns">
                  {#each columnsForTable(htMappingTable) as c}
                    <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                  {/each}
                </optgroup>
              </select>
            </label>

            <label>
              How are aliases stored?
              <span class="dau-field-hint">Most programs have one alias per row. Pick a different format if aliases are packed into a single field.</span>
              <select bind:value={htAliasFormat} class="webtv-input">
                <option value="single">One alias per row</option>
                <option value="text_array">Postgres text array</option>
                <option value="json_array">JSON array string</option>
                <option value="csv">Comma-separated string</option>
              </select>
            </label>
          </div>

          <div class="dau-column-pickers cols-2">
            <label>
              When did each claim start?
              <span class="dau-field-hint">We only count heartbeats after a user claims their alias — pick the timestamp that marks "this alias is mine."</span>
              <select bind:value={claimStartSource} class="webtv-input">
                <option value="column">From a column</option>
                <option value="fixed_date">Same date for everyone</option>
              </select>
            </label>
            {#if claimStartSource === 'column'}
              <label>
                Timestamp column
                <span class="dau-field-hint">Usually <code>created_at</code> — when the row was created</span>
                <select bind:value={claimStartColumn} class="webtv-input">
                  <option value="">Select...</option>
                  {#each columnsForTable(htMappingTable) as c}
                    <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                  {/each}
                </select>
              </label>
            {:else}
              <label>
                Fixed date
                <span class="dau-field-hint">All aliases count from this date — useful if there's no per-row timestamp</span>
                <input type="date" bind:value={claimStartDate} class="webtv-input" />
              </label>
            {/if}
          </div>
        {/if}
      </div>
    {/if}

    {#if hasCustomTime}
      <div class="dau-section">
        <h3>Custom Activity Table</h3>
        {#if activityTables.length > 0}
          <p class="dau-hint">These tables look like they might contain activity data:</p>
        {/if}
        <select bind:value={customTable} class="webtv-input">
          <option value="">Select a table...</option>
          {#if activityTables.length > 0}
            <optgroup label="Suggested">
              {#each activityTables as t}
                <option value={qname(t)}>{tableLabel(t)} ({t.row_count} rows)</option>
              {/each}
            </optgroup>
          {/if}
          <optgroup label="All tables">
            {#each tables as t}
              <option value={qname(t)}>{tableLabel(t)}</option>
            {/each}
          </optgroup>
        </select>

        {#if customTable}
          <div class="dau-column-pickers">
            <label>
              Who's the user?
              <span class="dau-field-hint">The column that identifies each person (email, slack ID, user_id FK, etc.)</span>
              <select bind:value={userColumn} class="webtv-input">
                <option value="">Select...</option>
                {#each userColumns(customTable) as c}
                  <option value={c.name}>{c.name}</option>
                {/each}
                <optgroup label="All columns">
                  {#each columnsForTable(customTable) as c}
                    <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                  {/each}
                </optgroup>
              </select>
            </label>

            {#if customUserIsId}
              <label>
                Which table has user emails?
                <span class="dau-field-hint">The user column looks like a foreign key. We need to join against the table that has the email address.</span>
                <select bind:value={emailTable} class="webtv-input">
                  <option value="">Select...</option>
                  {#each tables as t}
                    <option value={qname(t)}>{tableLabel(t)}</option>
                  {/each}
                </select>
              </label>
              {#if emailTable}
                <label>
                  Email column
                  <select bind:value={emailColumn} class="webtv-input">
                    <option value="">Select...</option>
                    {#each emailColumns(emailTable) as c}
                      <option value={c.name}>{c.name}</option>
                    {/each}
                    <optgroup label="All columns">
                      {#each columnsForTable(emailTable) as c}
                        <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                      {/each}
                    </optgroup>
                  </select>
                </label>
              {/if}
            {/if}

            <label>
              How long was each activity?
              <span class="dau-field-hint">The column holding duration/time spent — leave blank if rows don't have durations</span>
              <select bind:value={durationColumn} class="webtv-input">
                <option value="">(none)</option>
                {#each durationColumns(customTable) as c}
                  <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                {/each}
                <optgroup label="All columns">
                  {#each columnsForTable(customTable) as c}
                    <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                  {/each}
                </optgroup>
              </select>
            </label>

            <label>
              Duration unit
              <span class="dau-field-hint">What unit is the duration stored in?</span>
              <select bind:value={durationUnit} class="webtv-input">
                <option value="seconds">Seconds</option>
                <option value="minutes">Minutes</option>
                <option value="hours">Hours</option>
              </select>
            </label>
          </div>

          <div class="dau-column-pickers cols-2 spaced">
            <label>
              When did it happen?
              <span class="dau-field-hint">The timestamp column — we'll use this to group activity by day</span>
              <select bind:value={timestampColumn} class="webtv-input">
                <option value="">Select...</option>
                {#each timestampColumns(customTable) as c}
                  <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                {/each}
                <optgroup label="All columns">
                  {#each columnsForTable(customTable) as c}
                    <option value={c.name}>{c.name} ({c.udt || c.type})</option>
                  {/each}
                </optgroup>
              </select>
            </label>
          </div>

          <label class="dau-check spaced">
            <input type="checkbox" bind:checked={cap24h} />
            Cap at 24 hours per entry and per user-day
          </label>
        {/if}
      </div>
    {/if}

  <div class="dau-section">
    <label class="dau-check">
      <input type="checkbox" bind:checked={useCustomSql} />
      Write custom SQL (override auto-generated CTEs)
    </label>
    <p class="dau-hint">For programs that don't fit the pickers above, or when you need full control.</p>

    {#if useCustomSql}
      <div class="custom-sql-box">
        <details class="sql-guide" open>
          <summary>CTE format guide</summary>
          <div class="sql-guide-body">
            <p>Your SQL gets inserted into an existing <code>WITH</code> block. Write one or both CTEs below. End each with a trailing comma.</p>
            <p>Reference tables as <code>{"{{ source('" + sourceName + "', 'table_name') }}"}</code> (dbt source macro).</p>
            {#if isAirtable}
              <p>Airtable names are sanitized on the way into the warehouse — a table shown as <code>Time Entries</code> is <code>time_entries</code> here, and so are its fields. The copied prompt below lists the warehouse spellings.</p>
            {/if}

            {#if usesHackatime || (!usesHackatime && !hasCustomTime)}
            <div class="cte-spec">
              <h4>{programName}_ht_claims <span class="cte-spec-opt">— maps users to their Hackatime aliases</span></h4>
              <p class="cte-note">Must return exactly these columns:</p>
              <table class="cte-cols"><tbody>
                <tr><td><code>program_name</code></td><td>text</td><td>always <code>'{programName}'</code></td></tr>
                <tr><td><code>user_email</code></td><td>text</td><td>normalized email (see below)</td></tr>
                <tr><td><code>hackatime_alias</code></td><td>text</td><td>the project name they set in their editor</td></tr>
                <tr><td><code>project_name</code></td><td>text</td><td>human-readable project title, or NULL</td></tr>
                <tr><td><code>code_url</code></td><td>text</td><td>repo URL, or NULL</td></tr>
                <tr><td><code>claim_start_ts</code></td><td>timestamptz</td><td>when this alias became active (heartbeats before this are ignored)</td></tr>
              </tbody></table>
              <p class="cte-note">Filter out empty/null aliases.</p>
            </div>
            {/if}

            {#if hasCustomTime || (!usesHackatime && !hasCustomTime)}
            <div class="cte-spec">
              <h4>{programName}_custom_hourly <span class="cte-spec-opt">— aggregates non-Hackatime activity by hour</span></h4>
              <p class="cte-note">Must return exactly these columns:</p>
              <table class="cte-cols"><tbody>
                <tr><td><code>activity_hour</code></td><td>timestamptz</td><td><code>DATE_TRUNC('hour', your_ts AT TIME ZONE 'UTC')</code></td></tr>
                <tr><td><code>program_name</code></td><td>text</td><td>always <code>'{programName}'</code></td></tr>
                <tr><td><code>user_email</code></td><td>text</td><td>normalized email (see below)</td></tr>
                <tr><td><code>project_name</code></td><td>text</td><td>or NULL</td></tr>
                <tr><td><code>code_url</code></td><td>text</td><td>or NULL</td></tr>
                <tr><td><code>raw_hours_logged</code></td><td>numeric</td><td>total hours in this hour-bucket (sum durations, convert to hours)</td></tr>
                <tr><td><code>logging_method</code></td><td>text</td><td>always <code>'custom'</code></td></tr>
                <tr><td><code>source_detail</code></td><td>text</td><td>debug breadcrumb, e.g. <code>'{programName}.table.col; entries=' || COUNT(*)</code></td></tr>
              </tbody></table>
              <p class="cte-note">GROUP BY the first 5 columns. Filter out zero-duration rows.</p>
            </div>
            {/if}

            <div class="cte-spec">
              <h4>Email normalization</h4>
              <p class="cte-note">All <code>user_email</code> values must be normalized with this expression. Replace <code>email</code> with your column name:</p>
              <pre>CASE WHEN POSITION('@' IN LOWER(BTRIM(email))) &gt; 0
     THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)),
          '@', 1), '+', 1) || '@'
          || SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
     ELSE SPLIT_PART(LOWER(BTRIM(email)), '+', 1)
END</pre>
              <p class="cte-note">If users are identified by an integer ID, JOIN against the table that has their email and normalize that instead.</p>
            </div>
          </div>
        </details>

        <div class="custom-sql-prompt-bar">
          <span class="dau-hint">Or let an AI write it — copy this prompt, paste into Claude/ChatGPT, and paste the SQL it gives you below:</span>
          <button type="button" class="btn-sm" onclick={copyPrompt}>
            {promptCopied ? '✓ Copied!' : 'Copy Prompt'}
          </button>
        </div>

        <textarea
          bind:value={customSql}
          class="webtv-input custom-sql-textarea"
          placeholder={"-- Your CTE(s) here — replaces auto-generated DAU SQL\n" + programName + "_ht_claims AS (\n    ...\n),"}
          rows="14"
        ></textarea>

        {#if customSql.trim() && !isAirtable}
          <div class="test-sql-bar">
            <button type="button" class="btn-sm btn-test" onclick={testSql} disabled={testLoading}>
              {testLoading ? 'Running...' : 'Test SQL'}
            </button>
            <span class="dau-hint">Runs against the program database (read-only, 5s timeout)</span>
          </div>
        {/if}

        {#if testError}
          <div class="test-error">
            <pre>{testError}</pre>
          </div>
        {/if}

        {#if testResults}
          <div class="test-results">
            <div class="test-results-header">{testResults.rows.length} row{testResults.rows.length === 1 ? '' : 's'} returned</div>
            <div class="test-results-scroll">
              <table class="test-results-table">
                <thead>
                  <tr>
                    {#each testResults.columns as col}
                      <th>{col}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each testResults.rows as row}
                    <tr>
                      {#each row as cell}
                        <td>{cell === null ? 'NULL' : cell}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          </div>
        {/if}
      </div>
    {/if}
  </div>

  {#if missing.length > 0}
    <div class="dau-validation">
      {#each missing as m}
        <div>{m}</div>
      {/each}
    </div>
  {/if}

  <form action={formAction} method="post">
    <input type="hidden" name="authenticity_token" value={csrfToken} />
    {#if wid}<input type="hidden" name="wid" value={wid} />{/if}
    <input type="hidden" name="dau_config" value={buildConfig()} />
    <div class="dau-submit">
      {#if backUrl}
        <a href={backUrl} class="btn-back">← Back</a>
      {/if}
      <button type="submit" class="btn-chrome" disabled={missing.length > 0}>Continue →</button>
    </div>
  </form>
</div>

<style>
  .dau-wizard { font-size: 12px; }
  .dau-wizard p { font-size: 12px; }
  .dau-wizard h2 {
    margin: 0 0 6px;
    font-size: 16px;
    color: var(--gold-bright, #e0c840);
  }
  .dau-wizard h3 { font-size: 13px; font-weight: bold; margin: 0 0 6px; color: var(--gold, #c8a020); }

  .dau-section {
    margin: 16px 0;
    padding: 12px;
    border: 1px solid rgba(255,255,255,0.1);
    background: rgba(0,0,0,0.15);
  }

  .dau-dates {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
  }
  .dau-dates label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; }

  .dau-check {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    margin: 6px 0;
    cursor: pointer;
  }

  .dau-hint { color: #888899; font-size: 11px; margin: 0 0 8px; }
  .dau-field-hint { color: #666688; font-size: 10px; font-weight: normal; line-height: 1.3; }
  .dau-field-hint code { color: #888899; }

  .dau-column-pickers {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 10px;
    margin-top: 12px;
    align-items: start;
  }
  .dau-column-pickers.cols-2 { grid-template-columns: 1fr 1fr; }
  .dau-column-pickers.spaced { margin-top: 10px; }
  .dau-check.spaced { margin-top: 16px; }
  .dau-column-pickers label { display: flex; flex-direction: column; gap: 3px; font-size: 12px; font-weight: bold; color: var(--chrome-text, #d8d0c0); }

  .dau-column-pickers .webtv-input,
  .dau-dates .webtv-input {
    font-size: 12px;
    padding: 5px 6px;
    border-width: 2px;
  }

  .dau-validation {
    color: #ff4488;
    font-size: 14px;
    margin: 16px 0 0;
    padding: 8px 12px;
    border: 1px solid rgba(255, 68, 136, 0.3);
    background: rgba(255, 68, 136, 0.08);
  }

  .sql-guide {
    margin-bottom: 12px;
    border: 1px solid rgba(255,255,255,0.1);
    background: rgba(0,0,0,0.2);
  }
  .sql-guide summary {
    padding: 8px 12px;
    cursor: pointer;
    color: var(--gold, #c8a020);
    font-size: 14px;
    font-weight: bold;
  }
  .sql-guide-body {
    padding: 0 12px 12px;
    font-size: 13px;
  }
  .sql-guide-body p { margin: 8px 0; }
  .cte-spec { margin: 12px 0; }
  .cte-spec h4 {
    font-size: 13px;
    color: var(--gold-bright, #e0c840);
    margin: 0 0 4px;
  }
  .cte-spec-opt { color: #888899; font-weight: normal; font-size: 11px; }
  .cte-spec pre {
    background: #0a0a14;
    border: 1px solid rgba(255,255,255,0.1);
    padding: 8px 10px;
    font-family: 'Courier New', monospace;
    font-size: 11px;
    line-height: 1.5;
    color: #d8d0c0;
    overflow-x: auto;
    margin: 4px 0;
  }
  .cte-note { color: #888899; font-size: 12px; }

  .btn-sm {
    background: transparent;
    border: 1px solid var(--gold-dim, #8a6c18);
    color: var(--chrome-text, #d8d0c0);
    font-family: inherit;
    font-size: 12px;
    padding: 4px 12px;
    cursor: pointer;
    white-space: nowrap;
  }
  .btn-sm:hover { color: var(--gold-bright, #e0c840); border-color: var(--gold, #c8a020); }

  .custom-sql-box { margin-top: 12px; }
  .custom-sql-prompt-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
    gap: 12px;
  }
  .custom-sql-textarea {
    font-family: 'Courier New', 'Lucida Console', monospace;
    font-size: 12px;
    line-height: 1.5;
    resize: vertical;
    min-height: 120px;
    background: #0a0a14;
    color: #d8d0c0;
    border-color: var(--gold-dim, #8a6c18);
  }

  .dau-submit { display: flex; justify-content: space-between; margin-top: 20px; }
  .dau-submit button:disabled { opacity: 0.4; cursor: not-allowed; }

  .test-sql-bar {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 8px;
  }
  .btn-test {
    border-color: #33aa66;
    color: #33ff66;
  }
  .btn-test:hover:not(:disabled) { border-color: #33ff66; }
  .btn-test:disabled { opacity: 0.5; cursor: not-allowed; color: #888; border-color: #555; }

  .test-error {
    margin-top: 10px;
    padding: 8px 10px;
    border: 1px solid rgba(255, 68, 136, 0.4);
    background: rgba(255, 68, 136, 0.08);
  }
  .test-error pre {
    margin: 0;
    font-family: 'Courier New', monospace;
    font-size: 12px;
    color: #ff6699;
    white-space: pre-wrap;
    word-break: break-word;
  }

  .test-results {
    margin-top: 10px;
    border: 1px solid rgba(51, 255, 102, 0.2);
    background: #0a0a14;
  }
  .test-results-header {
    padding: 6px 10px;
    font-size: 12px;
    color: #33ff66;
    border-bottom: 1px solid rgba(51, 255, 102, 0.15);
    background: rgba(51, 255, 102, 0.05);
  }
  .test-results-scroll {
    overflow-x: auto;
    max-height: 360px;
    overflow-y: auto;
  }
  .test-results-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'Courier New', monospace;
    font-size: 11px;
  }
  .test-results-table th {
    position: sticky;
    top: 0;
    background: #12121e;
    color: var(--gold-bright, #e0c840);
    text-align: left;
    padding: 4px 8px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.15);
    white-space: nowrap;
  }
  .test-results-table td {
    padding: 3px 8px;
    color: #33ff66;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    white-space: nowrap;
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .test-results-table tr:hover td { background: rgba(51, 255, 102, 0.04); }
</style>
