import type * as types from "./types";

/**
 * This function will crawl through the components tree and grab any relevant pages
 * and append a list of sections to them
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
export const convertPixelsToRem = (px: number, doc?: Document): number => {
  // adding default to allow for testing where no document exists
  const computedStyle = doc
    ? parseFloat(
        getComputedStyle(document.documentElement).fontSize.replace("px", "")
      )
    : 16;

  return px / computedStyle;
};

/**
 * Our pathspecs are made up of flowname/runid/stepname/taskid,
 * where stepname/taskid are optional.  So to keep this testable and predictable,
 * we have 2 functions, one to get the object, and another to get a key.  You can
 * use whichever you need the data for
 */
export const getPathSpecObject = (pathspec: string): types.PathSpecObject => {
  const items = pathspec.split("/");

  return {
    flowname: items[0],
    runid: items[1],
    stepname: items?.[2],
    taskid: items?.[3],
  };
};

/**
 * This function will get a key value from the path spec object.
 */
export const getFromPathSpec = (
  pathspec?: string,
  key?: keyof types.PathSpecObject
): string | undefined => {
  if (!pathspec || !key) {
    return undefined;
  }

  return getPathSpecObject(pathspec)?.[key];
};

/* ------------------------------ SIDE EFFECTS ------------------------------ */

// Returns true if an element is overflown
export const isOverflown = (el: HTMLElement): boolean => {
  return el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth;
};

/**
 * Simple function to grab the section element and scroll to it.
 * Can be done after initial dom render.
 */
export const scrollToSection = (id: string): void => {
  const el = document.querySelector(`[data-section-id=${id}`);
  el?.scrollIntoView({ behavior: "smooth", block: "end", inline: "nearest" });
};
