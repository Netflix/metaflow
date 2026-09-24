<script lang="ts">
  // Make sure all standard models are registered
  import "@bokeh/bokehjs/build/js/lib/models/main"
  import "@bokeh/bokehjs/build/js/lib/models/widgets/main"
  import "@bokeh/bokehjs/build/js/lib/models/widgets/tables/main"
  import "@bokeh/bokehjs/build/js/lib/models/text/mathjax/main"

  import {Document} from "@bokeh/bokehjs/build/js/lib/document/document"
  import {add_document_standalone} from "@bokeh/bokehjs/build/js/lib/embed"
  import {isPlainObject} from "@bokeh/bokehjs/build/js/lib/core/util/types"

  import type {BokehEmbedComponent} from "../types"
  import {afterUpdate} from "svelte"

  export let componentData: BokehEmbedComponent

  let el: HTMLElement
  let doc: Document | null = null

  afterUpdate(async () => {
    //const {default: Bokeh} = await import("http://127.0.0.1:5777/static/js/bokeh.esm.js")
    //await import("http://127.0.0.1:5777/static/js/bokeh-widgets.esm.js")
    //await import("http://127.0.0.1:5777/static/js/bokeh-tables.esm.js")
    //await import("http://127.0.0.1:5777/static/js/bokeh-mathjax.esm.js")

    const {doc_json, patch_json} = componentData
    console.log("afterUpdate", doc_json, patch_json)

    //const {isPlainObject} = Bokeh.require("core/util/types")
    const clone_if_needed = <T>(obj: T) => {
      // DocJson and PatchJson are plain objects. However, we are getting objects from
      // different realms, which makes isPlainObject() ineffective. If that's the case,
      // then perform an inefficient, but always working clone via JSON serialization.
      if (isPlainObject(obj)) {
        return obj
      } else {
        return JSON.parse(JSON.stringify(obj))
      }
    }

    if (doc == null) {
      console.info("embedding", doc_json)
      doc = /*Bokeh.*/Document.from_json(clone_if_needed(doc_json))
      await /*Bokeh.embed.*/add_document_standalone(doc, el)
    }

    if (patch_json != null) {
      if (doc != null) {
        console.info("patching", patch_json)
        doc.apply_json_patch(clone_if_needed(patch_json))
      } else {
        console.warn("attempted to apply a patch before embedding")
      }
    }

  })
</script>

<div bind:this={el} data-component="bokeh"></div>
