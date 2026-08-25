<script>
  let { programName = '', previewUrl = '', formAction = '', backUrl = '', csrfToken = '', wid = '' } = $props()

  let loading = $state(true)
  let error = $state(null)
  let previewData = $state(null)
  let source = $state('')
  let warning = $state(null)

  async function fetchPreview() {
    loading = true
    error = null
    try {
      const res = await fetch(previewUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRF-Token': csrfToken,
        },
        body: JSON.stringify({ wid }),
      })
      const json = await res.json()
      error = json.error || null
      warning = json.warning || (json.truncated ? 'Only part of the source data was read — the chart undercounts activity.' : null)
      previewData = json.data || []
      source = json.source || 'unknown'
    } catch (e) {
      error = e.message
    } finally {
      loading = false
    }
  }

  $effect(() => { fetchPreview() })

  let maxDau = $derived(
    previewData ? Math.max(...previewData.map(d => d.dau || 0), 1) : 1
  )

  function barHeight(dau) {
    return Math.max((dau / maxDau) * 200, 2)
  }

  function formatDate(dateStr) {
    const d = new Date(dateStr + 'T00:00:00')
    return `${d.getMonth() + 1}/${d.getDate()}`
  }

  let showEveryN = $derived(
    previewData && previewData.length > 30 ? Math.ceil(previewData.length / 30) : 1
  )
</script>

<div class="dau-preview">
  <h2>DAU Preview</h2>
  <p>DAU projection for <strong>{programName}</strong>.</p>
  <p class="preview-disclaimer">Approximate — the real pipeline may differ slightly due to deduplication and timezone handling.</p>

  {#if loading}
    <div class="preview-loading">
      <h2 class="loading-title">CONNECTING TO YOUR DATABASE</h2>
      <div class="loading-city">
        <div class="city-skyline"></div>
        <div class="city-road">
          <div class="road-dash"></div>
        </div>
      </div>
      <p class="loading-text">Waiting for answer</p>
      <div class="loading-bar">
        <div class="loading-fill"></div>
      </div>
    </div>
  {:else if previewData && previewData.length > 0}
    {#if warning}
      <div class="webtv-banner banner-warn">Undercounted — {warning}</div>
    {/if}
    {#if error}
      <div class="webtv-banner banner-warn">Partial preview — one source failed: {error}</div>
    {/if}
    <div class="chart-meta">
      <span class="chart-source">
        Source: {source === 'blended' ? 'Hackatime + Custom' : source === 'hackatime' ? 'Hackatime' : 'Custom time'}
      </span>
      <span class="chart-range">
        {previewData[0]?.date} → {previewData[previewData.length - 1]?.date}
      </span>
    </div>

    <div class="chart-container">
      <div class="chart-y-axis">
        <span>{maxDau}</span>
        <span>{Math.round(maxDau / 2)}</span>
        <span>0</span>
      </div>
      <div class="chart-bars">
        {#each previewData as point, i (point.date)}
          <div class="bar-group" title="{point.date}: {point.dau} DAU">
            <div class="bar" style="height: {barHeight(point.dau)}px">
              {#if source === 'blended'}
                {#if point.custom > 0}
                  <div class="bar-segment bar-custom" style="height: {barHeight(point.custom)}px"></div>
                {/if}
                {#if point.hackatime > 0}
                  <div class="bar-segment bar-hackatime" style="height: {barHeight(point.hackatime)}px"></div>
                {/if}
              {/if}
            </div>
            {#if i % showEveryN === 0}
              <span class="bar-label">{formatDate(point.date)}</span>
            {/if}
          </div>
        {/each}
      </div>
    </div>

    {#if source === 'blended'}
      <div class="chart-legend">
        <span class="legend-item"><span class="legend-swatch swatch-custom"></span> Custom time</span>
        <span class="legend-item"><span class="legend-swatch swatch-hackatime"></span> Hackatime</span>
      </div>
    {/if}

    <div class="preview-verdict">
      <p>If this looks right, continue to review your generated code.</p>
    </div>
  {:else if error}
    <div class="webtv-banner banner-error">{error}</div>
    <p>Preview generation failed. You may proceed — configuration will be included in the PR for manual review.</p>
  {:else}
    <div class="webtv-banner banner-warn">No activity data found for the configured date range.</div>
    {#if warning}
      <div class="webtv-banner banner-warn">{warning}</div>
    {/if}
    <p>Possible causes: program not yet started, empty table. You may proceed.</p>
  {/if}

  <div class="preview-actions">
    <a href={backUrl || 'javascript:history.back()'} class="btn-back">← Back</a>
    <form action={formAction} method="get" style="display:inline">
      {#if wid}<input type="hidden" name="wid" value={wid} />{/if}
      <button type="submit" class="btn-chrome">Continue →</button>
    </form>
  </div>
</div>

<style>
  .dau-preview h2 {
    margin: 0 0 8px;
    font-size: 20px;
    color: var(--gold-bright, #e0c840);
    text-shadow: 0 0 8px rgba(200, 160, 32, 0.35);
    text-transform: uppercase;
    letter-spacing: 1px;
  }

  /* loading screen — WebTV "CONNECTING" aesthetic */
  .preview-loading {
    text-align: center;
    padding: 32px 0 48px;
  }

  .loading-title {
    font-size: 26px;
    font-weight: bold;
    color: #c8a020;
    text-shadow:
      0 0 10px rgba(200, 160, 32, 0.6),
      0 0 30px rgba(200, 160, 32, 0.3);
    letter-spacing: 3px;
    text-transform: uppercase;
    margin: 0 0 16px;
  }

  .loading-city {
    margin: 0 auto 24px;
    width: 400px;
    height: 180px;
    position: relative;
    overflow: hidden;
  }

  .loading-city::before {
    content: '';
    position: absolute;
    bottom: 50px;
    left: 50%;
    transform: translateX(-50%);
    width: 300px;
    height: 160px;
    background: radial-gradient(ellipse at 50% 80%, rgba(200, 160, 32, 0.4) 0%, rgba(200, 160, 32, 0.15) 40%, transparent 70%);
    pointer-events: none;
  }

  .city-skyline {
    position: absolute;
    bottom: 50px;
    left: 50%;
    transform: translateX(-50%);
    width: 280px;
    height: 80px;
    background: #111;
    clip-path: polygon(
      0% 100%, 0% 65%, 4% 65%, 4% 45%, 8% 45%, 8% 55%, 14% 55%, 14% 25%,
      18% 25%, 18% 20%, 22% 20%, 22% 50%, 26% 50%, 26% 35%, 32% 35%,
      32% 55%, 38% 55%, 38% 15%, 42% 15%, 42% 12%, 46% 12%, 46% 50%,
      50% 50%, 50% 40%, 54% 40%, 54% 30%, 58% 30%, 58% 55%, 62% 55%,
      62% 20%, 66% 20%, 66% 45%, 70% 45%, 70% 35%, 76% 35%, 76% 50%,
      80% 50%, 80% 60%, 86% 60%, 86% 45%, 92% 45%, 92% 55%, 96% 55%,
      96% 50%, 100% 50%, 100% 100%
    );
  }

  .city-skyline::after {
    content: '';
    position: absolute;
    inset: 0;
    background:
      radial-gradient(1px 1px at 10% 35%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 15% 55%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 20% 28%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 30% 42%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 40% 18%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 45% 35%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 55% 45%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 62% 25%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 68% 40%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 75% 38%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 82% 52%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 88% 48%, #33ff33 100%, transparent),
      radial-gradient(1px 1px at 92% 42%, #cc2200 100%, transparent),
      radial-gradient(1px 1px at 42% 14%, #cc2200 100%, transparent);
    clip-path: inherit;
  }

  .city-road {
    position: absolute;
    bottom: 0;
    left: 50%;
    transform: translateX(-50%);
    width: 80px;
    height: 50px;
    border-left: 2px solid #554400;
    border-right: 2px solid #554400;
    clip-path: polygon(35% 0%, 65% 0%, 100% 100%, 0% 100%);
  }

  .road-dash {
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    top: 0;
    width: 3px;
    height: 100%;
    background: repeating-linear-gradient(
      to bottom,
      #c8a020 0px, #c8a020 6px,
      transparent 6px, transparent 14px
    );
    animation: road-scroll 0.8s linear infinite;
  }

  @keyframes road-scroll {
    from { transform: translateX(-50%) translateY(-14px); }
    to { transform: translateX(-50%) translateY(0); }
  }

  .loading-text {
    color: #999;
    font-size: 16px;
    margin: 16px 0 12px;
  }

  .loading-bar {
    width: 260px;
    height: 18px;
    margin: 0 auto;
    border: 1px solid #334422;
    background: #0a0a00;
  }

  .loading-fill {
    height: 100%;
    background: repeating-linear-gradient(
      -45deg,
      #44aa44 0px, #44aa44 6px,
      #338833 6px, #338833 12px
    );
    background-size: 17px 18px;
    animation: fill-load 3s ease-in-out infinite, stripe-scroll 0.4s linear infinite;
  }

  @keyframes fill-load {
    0% { width: 5%; }
    50% { width: 75%; }
    100% { width: 100%; }
  }

  @keyframes stripe-scroll {
    from { background-position: 0 0; }
    to { background-position: 17px 0; }
  }

  .chart-meta {
    display: flex;
    justify-content: space-between;
    margin: 16px 0 8px;
    font-size: 16px;
    color: #888899;
  }

  .chart-container {
    display: flex;
    gap: 8px;
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid var(--chrome-border, #3a3a55);
    padding: 16px;
    min-height: 260px;
    align-items: flex-end;
  }

  .chart-y-axis {
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    height: 200px;
    font-size: 14px;
    color: #888899;
    text-align: right;
    min-width: 36px;
    padding-right: 4px;
    border-right: 1px solid #2a2a3a;
  }

  .chart-bars {
    display: flex;
    align-items: flex-end;
    gap: 2px;
    flex: 1;
    height: 220px;
    overflow-x: auto;
  }

  .bar-group {
    display: flex;
    flex-direction: column;
    align-items: center;
    flex: 1;
    min-width: 6px;
    max-width: 24px;
  }

  .bar {
    width: 100%;
    background: #33ff33;
    min-height: 2px;
    position: relative;
    transition: height 0.3s ease;
  }
  .bar:hover { background: #c8a020; }

  .bar-segment {
    width: 100%;
    position: absolute;
    bottom: 0;
    left: 0;
  }
  .bar-custom { background: #33ff33; }
  .bar-hackatime { background: #4499dd; }

  .bar-label {
    font-size: 11px;
    color: #888899;
    margin-top: 4px;
    white-space: nowrap;
    transform: rotate(-45deg);
    transform-origin: top left;
  }

  .chart-legend {
    display: flex;
    gap: 20px;
    margin-top: 12px;
    font-size: 16px;
  }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .legend-swatch { width: 16px; height: 16px; display: inline-block; }
  .swatch-custom { background: #33ff33; }
  .swatch-hackatime { background: #4499dd; }

  .preview-verdict { margin: 24px 0; }
  .preview-disclaimer { color: #666688; font-size: 13px; font-style: italic; margin-top: 4px; }

  .preview-actions {
    display: flex;
    justify-content: space-between;
    margin-top: 20px;
    padding-top: 16px;
    border-top: 1px solid rgba(255,255,255,0.08);
  }
</style>
