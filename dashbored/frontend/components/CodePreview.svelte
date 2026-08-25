<script>
  import hljs from 'highlight.js/lib/core'
  import python from 'highlight.js/lib/languages/python'
  import yaml from 'highlight.js/lib/languages/yaml'
  import sql from 'highlight.js/lib/languages/sql'

  hljs.registerLanguage('python', python)
  hljs.registerLanguage('yaml', yaml)
  hljs.registerLanguage('sql', sql)

  let { generated = {}, programName = '', formAction, backUrl = '', csrfToken, wid = '', validateUrl = '' } = $props()

  let submitting = $state(false)
  let validation = $state(null)
  let validating = $state(false)

  let isAirtable = $derived(generated.source_type === 'airtable')

  async function runValidation() {
    if (!validateUrl) return
    validating = true
    try {
      const res = await fetch(validateUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ wid }),
      })
      validation = await res.json()
    } catch (e) {
      validation = { _error: e.message }
    } finally {
      validating = false
    }
  }

  $effect(() => { runValidation() })

  // The server answers with {files}, {unavailable} or {error}. An empty file
  // set means nothing was checked, which is never a pass.
  let files = $derived(Object.entries(validation?.files ?? {}))
  let failure = $derived(validation?.error || validation?._error)
  let notRun = $derived(validation?.unavailable || (validation && !failure && files.length === 0))
  let allValid = $derived(files.length > 0 && files.every(([, r]) => r.ok))

  const slingTabs = [
    { key: 'assets_py', label: 'assets.py', lang: 'python' },
    { key: 'sources_yml', label: 'sources.yml', lang: 'yaml' },
    { key: 'dau_sql', label: 'DAU SQL', lang: 'sql' },
  ]

  const airtableTabs = [
    { key: 'base_config', label: 'airtable config', lang: 'python' },
    { key: 'dlt_sync_assets', label: 'dlt assets', lang: 'python' },
    { key: 'sources_yml', label: 'sources.yml', lang: 'yaml' },
    { key: 'dau_sql', label: 'DAU SQL', lang: 'sql' },
  ]

  let tabs = $derived(isAirtable ? airtableTabs : slingTabs)
  let visibleTabs = $derived(tabs.filter(t => generated[t.key]))

  let pickedTab = $state('')
  let activeTab = $derived(
    visibleTabs.some(t => t.key === pickedTab) ? pickedTab : visibleTabs[0]?.key || ''
  )
  let activeLang = $derived(tabs.find(t => t.key === activeTab)?.lang || 'python')

  function highlighted(code, lang) {
    if (!code) return '(no content)'
    try {
      return hljs.highlight(code, { language: lang }).value
    } catch {
      return code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    }
  }
</script>

<div class="code-preview">
  <h2>Review Generated Code</h2>
  <p>Generated files for <strong>{programName}</strong>. Review before submitting.</p>

  <div class="code-tabs">
    {#each visibleTabs as tab (tab.key)}
      <button
        class="code-tab"
        class:active={activeTab === tab.key}
        onclick={() => pickedTab = tab.key}
      >
        {tab.label}
      </button>
    {/each}
  </div>

  <div class="code-content">
    <pre><code>{@html highlighted(generated[activeTab], activeLang)}</code></pre>
  </div>

  {#if generated.dau_sql?.includes('NEEDS REVIEW')}
    <div class="webtv-banner banner-warn">
      Hackatime claims CTE needs manual review — every program maps aliases differently.
    </div>
  {/if}

  <div class="validation-bar">
    {#if validating}
      <span class="validation-loading">validating against real assets...</span>
    {:else if failure}
      <span class="validation-fail">⚠ validation failed: {failure}</span>
    {:else if notRun}
      <span class="validation-warn">
        ⚠ NOT VALIDATED — {notRun === true ? 'nothing was checked' : notRun}. Review this code by hand before shipping.
      </span>
    {:else if files.length}
      {#each files as [file, result] (file)}
        <span class="validation-badge" class:ok={result.ok} class:fail={!result.ok}>
          {result.ok ? '✓' : '✗'} {file}
        </span>
      {/each}
      {#if allValid}
        <span class="validation-summary">
          all {files.length} file{files.length === 1 ? '' : 's'} compile cleanly against the real codebase
        </span>
      {/if}
    {/if}
  </div>

  <form action={formAction} method="post" onsubmit={(e) => {
    if (submitting) { e.preventDefault(); return }
    if (!confirm('This will fork the repo and open a pull request. Ready?')) { e.preventDefault(); return }
    submitting = true
  }}>
    <input type="hidden" name="authenticity_token" value={csrfToken} />
    {#if wid}<input type="hidden" name="wid" value={wid} />{/if}
    <div class="code-submit">
      {#if backUrl}
        <a href={backUrl} class="btn-back">← Back</a>
      {/if}
      <p>This will fork <code>hackclub/data-warehouse</code> to your GitHub account and open a pull request.</p>
      <button type="submit" class="btn-chrome btn-large" disabled={submitting}>
        {submitting ? 'Creating PR...' : 'Open Pull Request →'}
      </button>
    </div>
  </form>
</div>

{#if submitting}
  <div class="connecting-overlay">
    <div class="connecting-bg">
      <img src="/static/img/skyline.png" alt="" class="connecting-skyline">
      <div class="connecting-text">SHIPPING YOUR CODE</div>
    </div>
  </div>
{/if}

<style>
  .code-preview h2 {
    margin: 0 0 8px;
    font-size: 18px;
    color: var(--gold-bright, #e0c840);
  }

  .code-tabs {
    display: flex;
    gap: 0;
    margin-top: 20px;
    border-bottom: 2px solid var(--gold-dim, #8a6c18);
  }

  .code-tab {
    background: rgba(0,0,0,0.3);
    border: 1px solid transparent;
    border-bottom: none;
    color: #888899;
    font-family: inherit;
    font-size: 14px;
    padding: 8px 20px;
    cursor: pointer;
  }
  .code-tab.active {
    background: #0a0a14;
    border-color: var(--gold-dim, #8a6c18);
    color: var(--gold-bright, #e0c840);
  }
  .code-tab:hover:not(.active) { color: var(--gold, #c8a020); }

  .code-content {
    background: #0a0a14;
    border: 1px solid var(--gold-dim, #8a6c18);
    border-top: none;
    padding: 16px;
    overflow-x: auto;
    max-height: 500px;
    overflow-y: auto;
  }
  .code-content pre {
    margin: 0;
    font-family: 'Courier New', 'Lucida Console', monospace;
    font-size: 13px;
    line-height: 1.5;
    color: #d8d0c0;
  }

  /* highlight.js theme — WebTV terminal palette */
  .code-content :global(.hljs-keyword) { color: #e0c840; }
  .code-content :global(.hljs-built_in) { color: #44cc88; }
  .code-content :global(.hljs-string) { color: #33ff33; }
  .code-content :global(.hljs-number) { color: #ff8844; }
  .code-content :global(.hljs-literal) { color: #ff8844; }
  .code-content :global(.hljs-title) { color: #4499dd; }
  .code-content :global(.hljs-title.function_) { color: #4499dd; }
  .code-content :global(.hljs-params) { color: #d8d0c0; }
  .code-content :global(.hljs-comment) { color: #666688; font-style: italic; }
  .code-content :global(.hljs-decorator) { color: #ff4488; }
  .code-content :global(.hljs-meta) { color: #ff4488; }
  .code-content :global(.hljs-attr) { color: #c8a020; }
  .code-content :global(.hljs-attribute) { color: #c8a020; }
  .code-content :global(.hljs-type) { color: #44cc88; }
  .code-content :global(.hljs-symbol) { color: #ff4488; }
  .code-content :global(.hljs-variable) { color: #d8d0c0; }
  .code-content :global(.hljs-punctuation) { color: #888899; }

  .code-submit {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 24px;
    gap: 16px;
  }
  .code-submit p { margin: 0; color: #888899; font-size: 18px; flex: 1; }
  .code-submit code { color: var(--gold, #c8a020); }

  .validation-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
    margin-top: 12px;
    min-height: 28px;
    font-size: 13px;
  }
  .validation-loading { color: #888899; font-style: italic; }
  .validation-fail { color: #ff6666; }
  .validation-warn { color: #e0c840; }
  .validation-badge {
    padding: 2px 10px;
    border-radius: 3px;
    font-family: 'Courier New', monospace;
  }
  .validation-badge.ok { color: #33ff33; background: rgba(51,255,51,0.1); }
  .validation-badge.fail { color: #ff6666; background: rgba(255,102,102,0.1); }
  .validation-summary { color: #33ff33; font-size: 12px; }

  .connecting-overlay {
    position: fixed;
    inset: 0;
    z-index: 9999;
    background: #000;
  }
  .connecting-bg {
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
  }
  .connecting-skyline {
    max-width: 80%;
    max-height: 60vh;
    object-fit: contain;
  }
  .connecting-text {
    font-family: Verdana, Arial, sans-serif;
    font-size: 14pt;
    color: #c8a020;
    letter-spacing: 3px;
    margin-top: 32px;
    animation: connecting-blink 1.2s step-end infinite;
  }
  @keyframes connecting-blink {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
  }
</style>
