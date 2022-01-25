/**
 * This is a sanity check to make sure the dev renders components.
 * Ideally, we should be doing component testing instead, however,
 * it appears that has a little way to go in development.
 *
 * For now, we're loading the dev page with the example card output, and
 * checking each component renders how we'd expect.
 */
import { metadata } from "../../public/card-example.json";

describe("Provides a sanity check on the demo page", () => {
  before(() => {
    cy.visit("/");
  });

  /* ---------------------------------- heading --------------------------------- */

  it("loads the heading component", () => {
    cy.get('[data-component="heading"]').contains(metadata.pathspec);
  });

  /* ----------------------------- navigation tree ---------------------------- */

  it("loads the navigation tree", () => {
    cy.get(".navList")
      .children()
      // nested nav list should have children
      .should("have.length", 2)
      .each((el) => {
        cy.wrap(el)
          // should have li children
          .find("ul li")
          .should("have.length.above", 0)
          .each((el) => {
            // each child should have a button inside it
            cy.wrap(el).find("button").should("have.length", 1);
          });
      });
  });

  /* ----------------------------- vertical table ----------------------------- */

  it("loads the vertical-table component", () => {
    cy.get('[data-component="table-vertical"]')
      .find("tr")
      .should("have.length.above", 2);
  });

  /* -------------------------------- artifacts ------------------------------- */

  it("loads the artifacts component", () => {
    cy.get('[data-component="artifacts"]')
      .find("table tr")
      .should("have.length.above", 2);
  });

  /* --------------------------------- images --------------------------------- */

  it("loads the images component", () => {
    cy.get('[data-component="image"]')
      .should("have.length", 3)
      .each((el) => {
        cy.wrap(el).get("figcaption").should("be.visible");
      });
  });

  /* ---------------------------- horizontal table ---------------------------- */

  it("loads the horizontal table component", () => {
    const table = cy.get('[data-component="table-horizontal"]');
    table.get("table").should("be.visible");
    table.get("thead").should("be.visible");
    table.get("tr").should("have.length.above", 2);
  });

  /* ----------------------------------- dag ---------------------------------- */

  it("loads the dag component", () => {
    const dag = cy.get('[data-component="dag"]');

    dag
      .get(".rectangle")
      .should("have.length.above", 2)
      .each((el, i, arr) => {
        const cyEl = cy.wrap(el);

        // all items should have a .name
        cyEl.get(".name").should("be.visible");

        // first item should be "start"
        if (i === 0) {
          cyEl.contains("start");
        }

        // last item should be "end"
        if (i === arr.length - 1) {
          cyEl.contains("end");
        }
      });

    dag.get(".path").should("have.length.above", 2);
    dag.get(".levelstoshow").should("have.length.above", 2);
    dag.get(".current").should("have.length", 1);
  });

  /* ----------------------------------- log ---------------------------------- */

  it("loads the log component", () => {
    cy.get('[data-component="log"]').find("code").should("have.length", 1);
  });

  /* ------------------------------- line chart ------------------------------- */
  it("loads the line chart component", () => {
    cy.get('[data-component="line-chart"]').get("canvas").should("be.visible");
  });

  /* -------------------------------- bar chart ------------------------------- */
  it("loads the bar chart component", () => {
    cy.get('[data-component="bar-chart"]').get("canvas").should("be.visible");
  });
});

export {};
