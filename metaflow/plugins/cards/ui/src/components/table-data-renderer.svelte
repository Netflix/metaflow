<!-- Renders a component or a primitive type-->
<script lang="ts">
  import type * as types from "../types";
  import type { ComponentType } from "svelte";
  import Artifacts from "./artifacts.svelte";
  import Dag from "./dag/dag.svelte";
  import Heading from "./heading.svelte";
  import Image from "./image.svelte";
  import VegaChart from "./vega-chart.svelte";
  import Log from "./log.svelte";
  import Markdown from "./markdown.svelte";
  import Text from "./text.svelte";
  import ProgressBar from "./progress-bar.svelte";
  import PythonCode from "./python-code.svelte";

  export let componentData: types.TableDataCell;
  let component: ComponentType;

  const typesMap: Record<string, ComponentType> = {
    artifacts: Artifacts,
    dag: Dag,
    heading: Heading,
    image: Image,
    log: Log,
    markdown: Markdown,
    progressBar: ProgressBar,
    text: Text,
    vegaChart: VegaChart,
    pythonCode: PythonCode,
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
