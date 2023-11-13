<!-- Renders a component or a primitive type-->
<script lang="ts">
  import type * as types from "../types";
  import type { ComponentType, SvelteComponentTyped } from "svelte";
  import Artifacts from "./artifacts.svelte";
  import BarChart from "./bar-chart.svelte";
  import Dag from "./dag/dag.svelte";
  import Heading from "./heading.svelte";
  import Image from "./image.svelte";
  import LineChart from "./line-chart.svelte";
  import VegaChart from "./vega-chart.svelte";
  import Log from "./log.svelte";
  import Markdown from "./markdown.svelte";
  import Text from "./text.svelte";
  import ProgressBar from "./progress-bar.svelte";

  export let componentData: types.TableDataCell;
  let component: ComponentType;

  const typesMap: Record<string, ComponentType> = {
    artifacts: Artifacts,
    barChart: BarChart,
    dag: Dag,
    heading: Heading,
    image: Image,
    lineChart: LineChart,
    log: Log,
    markdown: Markdown,
    progressBar: ProgressBar,
    text: Text,
    vegaChart: VegaChart,
  };

  const type = (componentData as types.CardComponent)?.type;

  if (type) {
    component = typesMap?.[type];
    if (!component) {
      console.error("Unknown component type: ", type);
    }
  }
</script>

{#if component}
  <svelte:component this={component} {componentData} />
{:else}
  {componentData}
{/if}
