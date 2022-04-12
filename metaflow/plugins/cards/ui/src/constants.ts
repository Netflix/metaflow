import type * as types from "./types";

// ROUTES

export const ROUTES: Record<string, types.Route> = {
  HOME: ["/", "Home"],
  CARD: ["/card", "Card"],
};

// COLORS

export const COLORS: Record<string, string> = {
  bg: "white",
  black: "#282828",
  blue: "rgb(12, 102, 222)",
  dkGrey: "#6a6a6a",
  dkPrimary: "#ef863b",
  dkSecondary: "#13172d0",
  dkTertiary: "#0f426e",
  grey: "#e9e9e9",
  highlight: "#f8d9d8",
  ltBlue: "rgb(228, 240, 255)",
  ltGrey: "#f7f7f7",
  ltPrimary: "#ffcb8b",
  ltSecondary: "#434d81",
  ltTertiary: "#4189c9",
  primary: "#faab4a",
  quadrary: "#f8d9d8",
  secondary: "#2e3454",
  tertiary: "#2a679d",
  success: "#2e8036",
  error: "#e13d3f",
};

export const COLORS_LIST: string[] = [
  COLORS.primary,
  COLORS.secondary,
  COLORS.tertiary,
  COLORS.quadrary,
];
