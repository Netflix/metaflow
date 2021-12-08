<script lang="ts">
  import "./global.css";
  import { cardData, modal } from "./store";
  import * as utils from "./utils";
  import * as constants from "./constants";
  import Aside from "./components/aside.svelte";
  import ComponentRenderer from "./components/card-component-renderer.svelte";
  import Logo from "./components/logo.svelte";
  import Main from "./components/main.svelte";
  import Modal from "./components/modal.svelte";
  import Nav from "./components/aside-nav.svelte";
</script>

<div class="container">
  <Aside>
    <div>
      <Nav pageHierarchy={utils.getPageHierarchy($cardData?.components)} />
    </div>
    <ul>
      <li><a href={constants.MF_CARD_DOCS_URL}>@card Docs</a></li>
      <li><a href={constants.OB_SLACK_URL}>Slack Help</a></li>
      <li><a href={constants.MF_GITHUB_URL}>Github</a></li>
    </ul>
  </Aside>

  <Main>
    <div class="logoHeader">
      <Logo />
    </div>
    {#each $cardData?.components || [] as componentData}
      <ComponentRenderer {componentData} />
    {/each}
  </Main>
</div>
<Modal componentData={$modal} />

<style>
  .container {
    width: 100%;
    height: 100vh;
    overflow: hidden;
    display: flex;
    flex-direction: row;
    position: relative;
  }

  .logoHeader {
    text-align: right;
    margin-bottom: 2rem;
  }

  :global(.logoHeader img) {
    width: 150px;
  }
</style>
