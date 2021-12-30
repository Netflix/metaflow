<!--Displays HTML written in markdown-->
<script lang="ts">
    import SvelteMarkdown from 'svelte-markdown'

    export let componentData: string;

</script>

<SvelteMarkdown source={componentData.source} />