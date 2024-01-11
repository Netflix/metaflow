<script lang="ts">
  import type { ComponentType } from "svelte";
  import type * as types from "../types";
  import Artifacts from "./artifacts.svelte";
  import Dag from "./dag/dag.svelte";
  import Heading from "./heading.svelte";
  import Image from "./image.svelte";
  import Log from "./log.svelte";
  import Markdown from "./markdown.svelte";
  import Page from "./page.svelte";
  import ProgressBar from "./progress-bar.svelte";
  import Section from "./section.svelte";
  import Subtitle from "./subtitle.svelte";
  import Table from "./table.svelte";
  import Text from "./text.svelte";
  import Title from "./title.svelte";
  import VegaChart from "./vega-chart.svelte";

  export let componentData: types.CardComponent;

  const typesMap: Record<typeof componentData.type, ComponentType> = {
    artifacts: Artifacts,
    dag: Dag,
    heading: Heading,
    image: Image,
    log: Log,
    markdown: Markdown,
    page: Page,
    progressBar: ProgressBar,
    section: Section,
    subtitle: Subtitle,
    table: Table,
    text: Text,
    title: Title,
    vegaChart: VegaChart,
  };

  let component = typesMap?.[componentData.type];
  if (!component) {
    console.error("Unknown component type: ", componentData.type);
  }
</script>

{#if component}
  {#if (componentData.type === "page" || componentData.type === "section") && componentData?.contents}
    <svelte:component this={component} {componentData}>
      <!-- if the component is a page or a section, we'll recursively add children to the slot -->
      {#each componentData.contents as child}
        <svelte:self componentData={child} />
      {/each}
    </svelte:component>
  {:else}
    <svelte:component this={component} {componentData} />
  {/if}
{/if}
