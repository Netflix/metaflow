<!-- A simple text component to render a paragraph wherever needed.App
  If you need more than one paragraph, you should probably use the HTML component instead -->
<script lang="ts">
  import type * as types from "../types";
  export let componentData: types.TextComponent;
  const { text } = componentData;
</script>

<p data-component="text">{text}</p>
