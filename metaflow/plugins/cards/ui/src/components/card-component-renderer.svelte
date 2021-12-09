<script lang="ts">
  import type * as types from "../types";
  import Artifacts from "./artifacts.svelte";
  import BarChart from "./bar-chart.svelte";
  import Dag from "./dag/dag.svelte";
  import Heading from "./heading.svelte";
  import Image from "./image.svelte";
  import LineChart from "./line-chart.svelte";
  import Log from "./log.svelte";
  import Page from "./page.svelte";
  import Section from "./section.svelte";
  import Subtitle from "./subtitle.svelte";
  import Table from "./table.svelte";
  import Text from "./text.svelte";
  import Title from "./title.svelte";

  export let componentData: types.CardComponent;

  const typesMap: Record<typeof componentData.type, any> = {
    artifacts: Artifacts,
    barChart: BarChart,
    dag: Dag,
    heading: Heading,
    image: Image,
    lineChart: LineChart,
    log: Log,
    page: Page,
    section: Section,
    subtitle: Subtitle,
    table: Table,
    text: Text,
    title: Title,
  };
</script>

{#if componentData?.type}
  <svelte:component
    this={typesMap?.[componentData.type] || Title}
    {componentData}
  />
{/if}
