import { ref } from 'vue';

// One fake WAL position shared by every homepage widget that shows an
// LSN (the hero statusline, the console window), so they can never
// display diverging positions. Starts at the last value the console's
// static startup history acknowledges, and only moves forward.
let hi = 0x16;
let lo = 0xb3762a94;
let timer: ReturnType<typeof setInterval> | undefined;
let subscribers = 0;

function format() {
  return `${hi.toString(16).toUpperCase()}/${lo
    .toString(16)
    .toUpperCase()
    .padStart(8, '0')}`;
}

export const lsn = ref(format());

function tick() {
  lo += 0x100 + Math.floor(Math.random() * 0x4000);
  if (lo > 0xffffffff) {
    lo -= 0x100000000;
    hi += 1;
  }
  lsn.value = format();
}

// Call from onMounted only (touches window). Idempotent across widgets;
// the ticker stops when the last subscriber unmounts.
export function subscribeLsn() {
  subscribers += 1;
  if (!timer && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    timer = setInterval(tick, 1200);
  }
}

export function unsubscribeLsn() {
  subscribers -= 1;
  if (subscribers <= 0 && timer) {
    clearInterval(timer);
    timer = undefined;
  }
}
