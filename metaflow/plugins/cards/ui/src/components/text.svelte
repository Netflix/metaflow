<!-- A simple text component to render a paragraph wherever needed.App
  If you need more than one paragraph, you should probably use the HTML component instead -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.TextComponent;
</script>

<p data-component="text">{componentData?.text || ""}</p>
