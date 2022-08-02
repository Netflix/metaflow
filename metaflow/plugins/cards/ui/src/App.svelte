<script lang="ts">
  // we are importing prism locally because its custom compiled.
  import "./prism";
  import "./global.css";
  import "./prism.css";
  import "./app.css";
  import { cardData, setCardData, modal } from "./store";
  import * as utils from "./utils";
  import Aside from "./components/aside.svelte";
  import ComponentRenderer from "./components/card-component-renderer.svelte";
  import Main from "./components/main.svelte";
  import Modal from "./components/modal.svelte";
  import Nav from "./components/aside-nav.svelte";
  import { onMount } from "svelte";

  export let cardDataId: string;

  let el: HTMLElement;
  const POST_MESSAGE_RESIZE_TYPE = "PluginHeightCheck";

  // Get the data from the element in `windows.__MF_DATA__` corresponding to `cardDataId`. This allows multiple sets of
  // data to exist on a single page
  setCardData(cardDataId);

  // Set the `embed` class to hide the `aside` if specified in the URL
  const urlParams = new URLSearchParams(window?.location.search);
  let embed = Boolean(urlParams.get("embed"));

  onMount(() => {
    // due to issue: https://github.com/Netflix/metaflow-ui/issues/74,
    // we need to tell the parent in a potential cross-domain iframe that the app div has changed.
    window?.parent.postMessage(
      {
        type: POST_MESSAGE_RESIZE_TYPE,
        height: Math.max(
          el?.scrollHeight ?? 0,
          el?.clientHeight ?? 0,
          el?.offsetHeight ?? 0
        ),
      },
      "*"
    );

    // lets continue to update the postMessage if the component changes size.
    try {
      const observer = new ResizeObserver(([entry]) => {
        window?.parent.postMessage(
          {
            type: POST_MESSAGE_RESIZE_TYPE,
            height: entry.contentRect.height,
          },
          "*"
        );
      });

      observer.observe(el);
    } catch (error) {
      console.error("Browser does not support ResizeObserver");
    }
  });
</script>

<div class="container mf-card" class:embed bind:this={el}>
  <Aside>
    <Nav pageHierarchy={utils.getPageHierarchy($cardData?.components)} />
  </Aside>

  <Main>
    {#each $cardData?.components || [] as componentData}
      <ComponentRenderer {componentData} />
    {/each}
  </Main>
</div>

{#if $modal}
  <Modal componentData={$modal} />
{/if}

<style>
  .container {
    width: 100%;
    display: flex;
    flex-direction: column;
    position: relative;
  }
</style>
