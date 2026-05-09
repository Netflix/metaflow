<script lang="ts">
  // Make sure all standard models are registered
  import "@bokeh/bokehjs/build/js/lib/models/main"
  import "@bokeh/bokehjs/build/js/lib/models/widgets/main"
  import "@bokeh/bokehjs/build/js/lib/models/widgets/tables/main"

  import {Document} from "@bokeh/bokehjs/build/js/lib/document"
  import {add_document_standalone} from "@bokeh/bokehjs/build/js/lib/embed"

  import type {BokehEmbedComponent} from "../types"
  import {onMount} from "svelte"

  export let componentData: BokehEmbedComponent

  const {doc_json} = componentData
  let el: HTMLElement

  onMount(() => {
    const document = Document.from_json(doc_json)
    void add_document_standalone(document, el)
  })
</script>

<div bind:this={el} data-component="bokeh"></div>
