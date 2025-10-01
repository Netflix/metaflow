<script lang="ts">
  import type * as types from "../types";
  import { onMount } from "svelte";
  
  export let componentData: types.JSONViewerComponent;

  $: ({ id, json_string, collapsible, show_copy_button, max_height, title } = componentData);
  
  let isCollapsed = false;
  let copySuccess = false;
  let copyTimeout: ReturnType<typeof setTimeout>;
  let codeElement: HTMLElement;
  
  // Copy to clipboard functionality
  async function copyToClipboard() {
    try {
      await navigator.clipboard.writeText(json_string);
      copySuccess = true;
      clearTimeout(copyTimeout);
      copyTimeout = setTimeout(() => {
        copySuccess = false;
      }, 2000);
    } catch (err) {
      console.error('Failed to copy: ', err);
    }
  }
  
  // Highlight code using Prism.js
  function highlightCode() {
    if (codeElement && (window as any)?.Prism) {
      (window as any).Prism.highlightElement(codeElement);
    }
  }
  
  // Re-highlight when content changes or component mounts
  $: if (codeElement && json_string) {
    highlightCode();
  }
  
  onMount(() => {
    highlightCode();
  });
  
  $: containerStyle = max_height ? `max-height: ${max_height}` : '';
</script>

<div class="json-viewer" {id}>
  <div class="json-header">
    {#if collapsible}
      <button 
        class="collapse-button" 
        on:click={() => isCollapsed = !isCollapsed}
        aria-label={isCollapsed ? 'Expand JSON' : 'Collapse JSON'}
      >
        <span class="collapse-icon" class:collapsed={isCollapsed}>▼</span>
        {title}
      </button>
    {:else}
      <span class="json-label">{title}</span>
    {/if}
    
    {#if show_copy_button}
      <button 
        class="copy-button" 
        on:click={copyToClipboard}
        class:success={copySuccess}
        title={copySuccess ? 'Copied!' : 'Copy to clipboard'}
      >
        {#if copySuccess}
          ✓ Copied
        {:else}
          📋 Copy
        {/if}
      </button>
    {/if}
  </div>
  
  {#if !isCollapsed}
    <div class="json-content" style={containerStyle}>
      <pre class="json-code"><code class="language-json" bind:this={codeElement}>{json_string}</code></pre>
    </div>
  {/if}
</div>

<style>
  .json-viewer {
    border: 1px solid #e5e7eb;
    border-radius: 0.375rem;
    background: #f9fafb;
    margin: 0.5rem 0;
    overflow: hidden;
  }
  
  .json-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0.75rem;
    background: #f3f4f6;
    border-bottom: 1px solid #e5e7eb;
    font-size: 0.875rem;
    font-weight: 500;
  }
  
  .collapse-button {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: none;
    border: none;
    color: #374151;
    cursor: pointer;
    font-size: 0.875rem;
    font-weight: 500;
  }
  
  .collapse-button:hover {
    color: #111827;
  }
  
  .collapse-icon {
    transition: transform 0.2s ease;
    font-size: 0.75rem;
  }
  
  .collapse-icon.collapsed {
    transform: rotate(-90deg);
  }
  
  .json-label {
    color: #374151;
    font-weight: 500;
  }
  
  .copy-button {
    background: #3b82f6;
    color: white;
    border: none;
    border-radius: 0.25rem;
    padding: 0.25rem 0.5rem;
    font-size: 0.75rem;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  
  .copy-button:hover {
    background: #2563eb;
  }
  
  .copy-button.success {
    background: #10b981;
  }
  
  .json-content {
    overflow: auto;
    max-height: 400px; /* Default max height */
  }
  
  .json-code {
    margin: 0;
    padding: 0;
    background: transparent;
    border: none;
    overflow: visible;
  }
  
  .json-code code {
    display: block;
    padding: 1rem;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.8125rem;
    line-height: 1.5;
    background: transparent;
    color: #374151;
    white-space: pre-wrap;
    word-break: break-word;
    border: none;
  }
  
  /* Let Prism.js handle all token styling - no custom overrides */
  
  /* Responsive adjustments */
  @media (max-width: 640px) {
    .json-header {
      padding: 0.375rem 0.5rem;
      font-size: 0.8125rem;
    }
    
    .json-code {
      padding: 0.75rem 0.5rem;
      font-size: 0.75rem;
    }
    
    .copy-button {
      padding: 0.1875rem 0.375rem;
      font-size: 0.6875rem;
    }
  }
  
  /* Table context adjustments */
  :global(table .json-viewer) {
    margin: 0.25rem 0;
    font-size: 0.75rem;
  }
  
  :global(table .json-content) {
    max-height: 200px;
  }
  
  :global(table .json-code) {
    padding: 0.5rem;
    font-size: 0.6875rem;
  }
</style>
