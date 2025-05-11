import { isNil, omitBy } from "lodash";

export function buildUrl(
  path: string,
  params?: Record<string, string | number | null | undefined>,
) {
  const queryString = new URLSearchParams(omitBy(params, isNil)).toString();
  return `${path}${queryString ? "?" + queryString : ""}`;
}

export function pluralise(count: number, singular: string, plural?: string) {
  const noun = count == 1 ? singular : plural || `${singular}s`;
  return `${count} ${noun}`;
}

export function choose(values: string[]): string {
  return values[Math.floor(Math.random() * values.length)];
}

export function humanSize(size: number) {
  var i = size == 0 ? 0 : Math.floor(Math.log(size) / Math.log(1024));
  return [
    Number((size / Math.pow(1024, i)).toFixed(2)),
    ["bytes", "kB", "MB", "GB", "TB"][i],
  ].join(" ");
}

export function truncatePath(path: string) {
  const parts = path.split("/");
  if (parts.length <= 3) {
    return path;
  }
  return [
    parts[0],
    ...parts.slice(1, -1).map((p) => p[0]),
    parts[parts.length - 1],
  ].join("/");
}

const adjectives = [
  "bewitching",
  "captivating",
  "charming",
  "clever",
  "enchanting",
  "funny",
  "goofy",
  "happy",
  "jolly",
  "lucky",
  "majestic",
  "mysterious",
  "mystical",
  "playful",
  "quirky",
  "silly",
  "sleepy",
  "soothing",
  "whimsical",
  "witty",
  "zany",
];

const properNames = [
  "banshee",
  "centaur",
  "chimera",
  "chupacabra",
  "cyclops",
  "djinn",
  "dragon",
  "fairy",
  "gargoyle",
  "genie",
  "gnome",
  "goblin",
  "griffin",
  "grizzly",
  "gryphon",
  "hydra",
  "kraken",
  "leprechaun",
  "mermaid",
  "minotaur",
  "mothman",
  "nymph",
  "ogre",
  "oracle",
  "pegasus",
  "phantom",
  "phoenix",
  "pixie",
  "sasquatch",
  "satyr",
  "shapeshifter",
  "siren",
  "sorcerer",
  "spectre",
  "sphinx",
  "troll",
  "unicorn",
  "vampire",
  "warlock",
  "werewolf",
  "wizard",
  "yeti",
];

export function randomName(): string {
  return `${choose(adjectives)}_${choose(properNames)}`;
}

export function removeAt<T>(array: T[], index: number) {
  return [...array.slice(0, index), ...array.slice(index + 1)];
}

export function insertAt<T>(array: T[], index: number, value: T): T[] {
  return [...array.slice(0, index), value, ...array.slice(index)];
}
