import type * as types from "./types";
import type { VisualizationSpec } from "svelte-vega";
import { writable } from "svelte/store";
import type { Writable } from "svelte/store";

export const cardData: Writable<types.CardResponse | undefined> =
  writable(undefined);

/**
 * This function is used to update the cardData object from the window.
 * It is used to update the cardData object with
 * changes that have come from the parent window.
 */
(window as any).metaflow_card_update = (
  dataChanges: Record<string, types.CardComponent>
) => {
  cardData?.update((d: any) => {
    const newData: types.CardResponse = { ...d };

    Object.values(dataChanges).forEach(
      (change) =>
        newData?.components && findAndMutateTree(newData.components, change)
    );

    return newData;
  });
  return true;
};

const mutateChartElement = (
  chart: types.VegaChartComponent,
  newChart: types.VegaChartComponent
) => {
  if (chart.data) {
    chart.data = JSON.parse(JSON.stringify(newChart.data)) as Record<
      string,
      unknown
    >;
  }
  const specHasNotChanged =
    JSON.stringify(newChart.spec) === JSON.stringify(chart.spec);

  if (!specHasNotChanged) {
    chart.spec = JSON.parse(JSON.stringify(newChart.spec)) as VisualizationSpec;
  }
};

// NOTE: this function mutates the object! Be careful with it.
const findAndMutateTree = (
  components: types.CardComponent[],
  newComponent: types.CardComponent
) => {
  const componentIndex = components.findIndex(
    (fcomp) => newComponent.id === fcomp?.id
  );

  if (componentIndex > -1) {
    if (components[componentIndex].type == "vegaChart") {
      // if the component is a vegaChart, we need to merge the data
      mutateChartElement(
        components[componentIndex] as types.VegaChartComponent,
        newComponent as types.VegaChartComponent
      );
    } else {
      Object.assign(components[componentIndex], newComponent);
    }
  } else {
    // if the component has children, and nothing was found, run again with them
    components.forEach((component) => {
      if (
        (component.type === "section" || component.type === "page") &&
        component?.contents?.length
      ) {
        findAndMutateTree(component.contents, newComponent);
      }
    });
  }
};

// Fetch the data from the window, or fallback to the example data file
export const setCardData: (cardDataId: string) => void = (cardDataId) => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    const data = JSON.parse(
      atob((window as any).__MF_DATA__[cardDataId])
    ) as types.CardResponse;
    cardData.set(data);
  } catch (error) {
    // for now, we are loading an example card if there is no string
    fetch("/card-example.json")
      .then((resp) => resp.json())
      .then((data: types.CardResponse) => {
        cardData.set(data);
      })
      .catch(console.error);
  }
};

export const modal: Writable<types.CardComponent | undefined> =
  writable(undefined);
