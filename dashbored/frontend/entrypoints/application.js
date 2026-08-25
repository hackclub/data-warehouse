import '../styles/webtv.css'
import { mount } from 'svelte'

const components = import.meta.glob('../components/*.svelte')

document.querySelectorAll('[data-svelte-component]').forEach(async (el) => {
  const name = el.dataset.svelteComponent
  const props = JSON.parse(el.dataset.svelteProps || '{}')
  const path = `../components/${name}.svelte`

  if (components[path]) {
    const mod = await components[path]()
    mount(mod.default, { target: el, props })
  } else {
    console.warn(`[dashbored] svelte component not found: ${name}`)
  }
})
