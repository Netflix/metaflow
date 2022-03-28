import * as utils from "../../src/utils";
import { components, metadata } from "../../public/card-example.json";
import type * as types from "../../src/types";

describe("utils", () => {
  it("should getPageHierarchy", () => {
    const expected = {
      Run: ["Vertical Table", "Artifacts", "Images", "Horizontal Table"],
      Task: ["DAG", "Log Component", "Line Chart", "Bar Chart"],
    };

    expect(utils.getPageHierarchy(components as types.CardComponent[])).to.eql(
      expected
    );
  });

  it("should convertPixelsToRem", () => {
    expect(utils.convertPixelsToRem(16)).to.eq(1);
    expect(utils.convertPixelsToRem(10)).to.eq(0.625);
  });

  it("should getPathSpecObject", () => {
    expect(utils.getPathSpecObject(metadata.pathspec)).to.eql({
      flowname: "DefaultCardFlow",
      runid: "1635187021511332",
      stepname: "join_static",
      taskid: "1",
    });

    expect(utils.getPathSpecObject("JSONParameterFlow/5536")).to.eql({
      flowname: "JSONParameterFlow",
      runid: "5536",
      stepname: undefined,
      taskid: undefined,
    });
  });

  it("should getFromPathSpec", () => {
    // DefaultCardFlow/1635187021511332/join_static/1
    const flowname = utils.getFromPathSpec(metadata.pathspec, "flowname");
    expect(flowname).to.eql("DefaultCardFlow");

    const runid = utils.getFromPathSpec(metadata.pathspec, "runid");
    expect(runid).to.equal("1635187021511332");

    const stepname = utils.getFromPathSpec(metadata.pathspec, "stepname");
    expect(stepname).to.equal("join_static");

    const taskid = utils.getFromPathSpec(metadata.pathspec, "taskid");
    expect(taskid).to.equal("1");
  });

  // SIDE EFFECTS, can't really mock these in unit tests, so lets just make sure they exist
  it("should have side-effect functions", () => {
    expect(utils.isOverflown).to.exist;
    expect(utils.scrollToSection).to.exist;
  });
});

export {};
