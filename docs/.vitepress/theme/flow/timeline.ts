import { createTimeline, type Timeline, type TimelineParams } from 'animejs';

// Lays back-to-back phases out on one anime.js timeline, so pausing it
// freezes the whole cycle. Each phase is labelled with its name. Cues are
// one-off callbacks at a point in time; tweens get linear 0..1 progress.

export type Cue = [offset: number, fn: () => void];
export type Tween = [offset: number, ms: number, fn: (v: number) => void];

export interface Phase {
  name: string;
  ms: number; // until the next phase starts
  run?: () => void; // at the phase's start
  at?: Cue[]; // relative to the phase's start
  tweens?: Tween[]; // relative to the phase's start
}

export function phaseTimeline(phases: Phase[], params: TimelineParams = {}): Timeline {
  const tl = createTimeline({ autoplay: false, composition: false, ...params });
  let t = 0;
  for (const p of phases) {
    tl.label(p.name, t);
    if (p.run) tl.call(p.run, t);
    for (const [offset, fn] of p.at ?? []) tl.call(fn, t + offset);
    for (const [offset, ms, fn] of p.tweens ?? []) {
      tl.add({ duration: ms, onUpdate: s => fn(s.progress) }, t + offset);
    }
    t += p.ms;
  }
  // holds the timeline open until the last phase ends
  return tl.call(() => {}, t);
}
