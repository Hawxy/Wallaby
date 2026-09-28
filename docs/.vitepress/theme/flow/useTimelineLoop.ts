import { onMounted, onUnmounted, watch, type Ref } from 'vue';
import type { Timeline } from 'animejs';

// Shared playback for the animated diagrams: plays a chain of timelines,
// asking `next` for the following one each time a cycle completes. The
// chain only runs while the element is on screen in a visible tab, the
// optional `active` flag is set (a user's pause), and motion isn't
// reduced; pausing freezes the current timeline mid-flight.
export function useTimelineLoop(
  el: Ref<HTMLElement | undefined>,
  next: (first: boolean) => Timeline,
  opts: { active?: Ref<boolean>; threshold?: number } = {},
) {
  let current: Timeline | undefined;
  let started = false;
  let running = false;
  let inView = false;
  let reduced = false;
  let observer: IntersectionObserver | undefined;

  function start() {
    const tl = next(!started);
    started = true;
    current = tl;
    tl.then(() => {
      if (current === tl) start();
    });
    if (running) tl.play();
  }

  function sync() {
    const run = inView && !document.hidden && !reduced && (opts.active?.value ?? true);
    if (run === running) return;
    running = run;
    if (!run) current?.pause();
    else if (current) current.play();
    else start();
  }

  // drops the current timeline; the next one starts right away if playing,
  // or on the next play otherwise
  function reset() {
    current?.cancel();
    current = undefined;
    if (running) start();
  }

  if (opts.active) watch(opts.active, sync);

  onMounted(() => {
    reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    observer = new IntersectionObserver(([entry]) => {
      inView = entry.isIntersecting;
      sync();
    }, { threshold: opts.threshold ?? 0 });
    if (el.value) observer.observe(el.value);
    document.addEventListener('visibilitychange', sync);
  });

  onUnmounted(() => {
    running = false;
    current?.cancel();
    current = undefined;
    observer?.disconnect();
    document.removeEventListener('visibilitychange', sync);
  });

  return {
    reset,
    isRunning: () => running,
    isReduced: () => reduced,
  };
}
