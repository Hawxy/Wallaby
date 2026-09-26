// A step scheduler on a caller-owned clock: the caller advances time
// (so pausing the clock pauses everything) and phases play back to back.

export interface Phase {
  ms: number; // until the next phase starts
  run?: () => void; // at the phase's start
  at?: [offset: number, fn: () => void][]; // relative to the phase's start
}

export function createTimeline() {
  let steps: { at: number; fn: () => void }[] = [];

  function schedule(at: number, fn: () => void) {
    steps.push({ at, fn });
  }

  return {
    schedule,

    // plays phases back to back from start; returns their total length
    play(start: number, phases: Phase[]) {
      let t = start;
      for (const p of phases) {
        if (p.run) schedule(t, p.run);
        for (const [offset, fn] of p.at ?? []) schedule(t + offset, fn);
        t += p.ms;
      }
      return t - start;
    },

    // fires every due step in time order (ties keep scheduling order),
    // including steps that due steps schedule
    runDue(now: number) {
      while (steps.some(s => s.at <= now)) {
        const due = steps.filter(s => s.at <= now).sort((a, b) => a.at - b.at);
        steps = steps.filter(s => s.at > now);
        due.forEach(s => s.fn());
      }
    },
  };
}
