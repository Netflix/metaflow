<!--Displays HTML written in markdown-->
<script lang="ts">
    import SvelteMarkdown from 'svelte-markdown'
    import type { MarkdownComponent } from '../types';

    export let componentData: MarkdownComponent
</script>

<SvelteMarkdown source={componentData.source} />