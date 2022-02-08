<!-- Renders a component or a primitive type-->
<script lang="ts">
    import type * as types from "../types";
    import type { SvelteComponentDev } from "svelte/internal";
    import Artifacts from "./artifacts.svelte";
    import BarChart from "./bar-chart.svelte";
    import Dag from "./dag/dag.svelte";
    import Heading from "./heading.svelte";
    import Image from "./image.svelte";
    import LineChart from "./line-chart.svelte";
    import Log from "./log.svelte";
    import Markdown from "./markdown.svelte";
    import Text from "./text.svelte";

    export let componentData: types.TableDataCell | boolean | string | number;
    let component: typeof SvelteComponentDev;
  
    const typesMap: Record<string, typeof SvelteComponentDev> =
    {
        artifacts: Artifacts,
        barChart: BarChart,
        dag: Dag,
        heading: Heading,
        image: Image,
        lineChart: LineChart,
        log: Log,
        markdown: Markdown,
        text: Text,
    };

    const type = (componentData as types.CardComponent)?.type
    if (type) {
        component = typesMap?.[type]
        if (!component) {
            console.error("Unknown component type: ", type)
        }
    }
</script>

{#if component }
    <svelte:component this={component} {componentData} />
{:else}
    {componentData}
{/if}
