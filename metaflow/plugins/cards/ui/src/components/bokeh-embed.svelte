<script lang="ts">
  // Make sure all standard models are registered
  //import "@bokeh/bokehjs/build/js/lib/models/main"
  //import "@bokeh/bokehjs/build/js/lib/models/widgets/main"
  //import "@bokeh/bokehjs/build/js/lib/models/widgets/tables/main"

  import type {Document} from "@bokeh/bokehjs/build/js/lib/document/document"
  //import {add_document_standalone} from "@bokeh/bokehjs/build/js/lib/embed"

  import type {BokehEmbedComponent} from "../types"
  import {afterUpdate} from "svelte"

  export let componentData: BokehEmbedComponent

  let el: HTMLElement
  let doc: Document | null = null

  afterUpdate(async () => {
    const {default: Bokeh} = await import("http://127.0.0.1:5777/static/js/bokeh.esm.js")
    await import("http://127.0.0.1:5777/static/js/bokeh-widgets.esm.js")
    await import("http://127.0.0.1:5777/static/js/bokeh-tables.esm.js")
    await import("http://127.0.0.1:5777/static/js/bokeh-mathjax.esm.js")
    console.log(Bokeh)

    const {doc_json, patch_json} = componentData
    console.log("afterUpdate", doc_json, patch_json, doc)

    if (doc == null) {
      const cloned_doc_json = JSON.parse(JSON.stringify(doc_json))
      doc = Bokeh.Document.from_json(cloned_doc_json)
      console.info("embedding", doc_json)
      await Bokeh.embed.add_document_standalone(doc, el)
    }

    if (patch_json != null) {
      if (doc != null) {
        const cloned_patch_json = JSON.parse(JSON.stringify(patch_json))
        console.info("patching", patch_json)
        doc.apply_json_patch(cloned_patch_json)
      } else {
        console.warn("attempted to apply a patch before embedding")
      }
    }

    /* TODO desired logic
    if (doc_json == null && patch_json == null) {
      console.warn("nothing to do")
      return
    }

    if (doc_json != null) {
      if (doc != null) {
        doc.clear()
      }
      doc = Bokeh.Document.from_json(doc_json)
      await Bokeh.embed.add_document_standalone(doc, el)
    }

    if (patch_json != null) {
      if (doc != null) {
        const cloned_patch_json = JSON.parse(JSON.stringify(patch_json))
        doc.apply_json_patch(cloned_patch_json)
      } else {
        console.warn("attempted to apply a patch before embedding")
      }
    }
    */
  })
</script>

<div bind:this={el} data-component="bokeh"></div>
