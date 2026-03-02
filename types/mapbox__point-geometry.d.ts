// Stub types for @mapbox/point-geometry — included by @types/mapbox-gl
declare module "@mapbox/point-geometry" {
  export default class Point {
    x: number;
    y: number;
    constructor(x: number, y: number);
    clone(): Point;
    add(p: Point): Point;
    sub(p: Point): Point;
    mult(k: number): Point;
    div(k: number): Point;
    rotate(angle: number): Point;
    matMult(m: [number, number, number, number]): Point;
    unit(): Point;
    perp(): Point;
    round(): Point;
    mag(): number;
    equals(p: Point): boolean;
    dist(p: Point): number;
    distSqr(p: Point): number;
    angle(): number;
    angleTo(p: Point): number;
    angleWith(b: Point): number;
    angleWithSep(x: number, y: number): number;
    static convert(a: [number, number] | { x: number; y: number } | Point): Point;
  }
}
