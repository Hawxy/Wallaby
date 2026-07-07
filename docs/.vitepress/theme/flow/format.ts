// thousands separators for the delivered counters - 1,234,567
export function formatCount(n: number) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}
