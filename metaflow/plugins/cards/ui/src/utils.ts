import type * as types from "./types";

/**
 * Simple function to grab the section element and scroll to it.
 * Can be done after initial dom render.
 */
export const scrollToSection = (id: string): void => {
  const el = document.querySelector(`[data-section-id=${id}`);
  el?.scrollIntoView({ behavior: "smooth", block: "end", inline: "nearest" });
};

/**
 * This function will crawl through the components tree and
 * grab any relevant pages and sections.  It should note, its not actually recursive, because we only
 * support one level of page/sections, however, we could easily make this work with nesting
 */
export const getPageHierarchy = (
  components?: types.CardComponent[]
): types.PageHierarchy => {
  const hierarchy: types.PageHierarchy = {};
  if (!components) return hierarchy;

  function addSections(
    component: types.CardComponent,
    sectionsArr: string[] = []
  ) {
    if (component.type === "page") {
      const s: string[] = [];
      hierarchy[component.title] = s;
      component?.contents?.forEach((c) => addSections(c, s));
    }

    if (component.type === "section" && component.title) {
      sectionsArr.push(component.title);
    }
  }

  components?.forEach((c) => addSections(c));

  return hierarchy;
};

/**
 * Function to calculate pixels from computed rem size
 */
export const convertPixelsToRem = (px: number): number => {
  return (
    px /
    parseFloat(
      getComputedStyle(document.documentElement).fontSize.replace("px", "")
    )
  );
};

// Parses a path spec and returns the step name
// This may be refactored if there is a need to pull out the flow/run/task
export const getStepNameFromPathSpec = (
  pathspec?: string
): string | undefined => {
  if (!pathspec) {
    return undefined;
  }
  const step = pathspec.split("/")?.[2];
  return step;
};

// Returns true if an element is overflown
export const isOverflown = (el: HTMLElement): boolean => {
  return el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth;
};
