<!-- Renders a component or a primitive type-->
<script lang="ts">
    import type * as types from "../types";
    import ComponentRenderer from "./card-component-renderer.svelte";

    export let componentData: types.CardComponent | boolean | string | number;
    let type = typeof componentData
    let cardComponent = componentData as types.CardComponent
</script>

{#if type !== "object" }
    {componentData}
{:else}
    <ComponentRenderer componentData={cardComponent}/>
{/if}