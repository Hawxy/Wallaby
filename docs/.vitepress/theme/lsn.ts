import { ref } from 'vue';

// The homepage's fake WAL position. It only advances when the hero
// pipeline runs its pulse choreography (tickLsn at packet launch), so
// the position, the moving packet, and the delivered counter always
// tell one story. Deterministic start - SSR hydration.
let hi = 0x16;
let lo = 0xb3762a94;

function format() {
  return `${hi.toString(16).toUpperCase()}/${lo
    .toString(16)
    .toUpperCase()
    .padStart(8, '0')}`;
}

export const lsn = ref(format());

export function tickLsn() {
  lo += 0x100 + Math.floor(Math.random() * 0x4000);
  if (lo > 0xffffffff) {
    lo -= 0x100000000;
    hi += 1;
  }
  lsn.value = format();
}
