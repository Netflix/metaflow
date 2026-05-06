<!-- Draws all the connectors between Steps in a DAG -->
<script lang="ts">
  import type { DagStructure } from "../../types";
  import { convertPixelsToRem } from "../../utils";
  import Connector from "./connector.svelte";

  export let dagStructure: DagStructure;
  export let container: HTMLElement;

  interface ConnectorData {
    top: number;
    left: number;
    bottom: number;
    right: number;
  }

  let connectors: ConnectorData[] = [];

  $: {
    connectors = [];
    if (container) {
      const containerBox = container.getBoundingClientRect();
      const top = containerBox.top;
      const left = containerBox.left;

      Object.values(dagStructure).forEach((nodeData) => {
        const nodeRect = nodeData.node.getBoundingClientRect();
        nodeData.connections?.forEach((str) => {
          const connectionNode = dagStructure[str]
          if (!connectionNode) {
            console.warn("Connection node not found:", str);
            return;
          }
          const connectionRect = connectionNode.node.getBoundingClientRect();
          const newConnectorData: ConnectorData = {
            top: nodeRect.bottom - top,
            left: nodeRect.left - left + nodeRect.width / 2,
            bottom: connectionRect.top - top,
            right: connectionRect.left - left + connectionRect.width / 2,
          };
          connectors = [...connectors, newConnectorData];
        });
      });
    }

  }
</script>

{#each connectors as connector}
  <Connector
    top={convertPixelsToRem(connector.top)}
    left={convertPixelsToRem(connector.left)}
    bottom={convertPixelsToRem(connector.bottom)}
    right={convertPixelsToRem(connector.right)}
  />
{/each}
