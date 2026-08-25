<script>
  let { bases = [], formAction, wid = '' } = $props()

  let query = $state('')

  let filtered = $derived(
    query.trim()
      ? bases.filter(b =>
          b.name.toLowerCase().includes(query.toLowerCase()) ||
          b.id.toLowerCase().includes(query.toLowerCase())
        )
      : bases
  )
</script>

<div class="base-picker">
  <input
    type="text"
    class="webtv-input base-search"
    placeholder="search bases..."
    bind:value={query}
  />

  <div class="base-count">
    {filtered.length} of {bases.length} base{bases.length === 1 ? '' : 's'}
  </div>

  <div class="base-list">
    {#each filtered as base (base.id)}
      <form action={formAction} method="post" class="base-row">
        <input type="hidden" name="wid" value={wid} />
        <input type="hidden" name="base_id" value={base.id} />
        <input type="hidden" name="base_name" value={base.name} />
        <div class="base-info">
          <span class="base-name">{base.name}</span>
          <span class="base-id">{base.id}</span>
        </div>
        <button type="submit" class="btn-chrome">Use this base &rarr;</button>
      </form>
    {/each}
    {#if filtered.length === 0}
      <div class="base-empty">
        No bases match "{query}"
      </div>
    {/if}
  </div>
</div>

<style>
  .base-picker {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .base-search {
    width: 100%;
    box-sizing: border-box;
  }

  .base-count {
    font-size: 11px;
    color: #666688;
    font-family: Verdana, Arial, sans-serif;
  }

  .base-list {
    max-height: 400px;
    overflow-y: auto;
    border: 1px solid #3a3a55;
  }

  .base-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 12px;
    border-bottom: 1px solid #2a2a3e;
  }
  .base-row:last-child { border-bottom: none; }
  .base-row:hover { background: rgba(200, 160, 32, 0.06); }

  .base-info {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
    flex: 1;
  }

  .base-name {
    font-family: Verdana, Arial, sans-serif;
    font-size: 13px;
    color: #d8d0c0;
    font-weight: bold;
  }

  .base-id {
    font-family: 'Courier New', monospace;
    font-size: 11px;
    color: #666688;
  }

  .base-empty {
    padding: 20px;
    text-align: center;
    color: #666688;
    font-family: Verdana, Arial, sans-serif;
    font-size: 13px;
  }
</style>
