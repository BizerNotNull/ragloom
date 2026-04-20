export function Greet({ name }: { name: string }) {
  return <span>hello, {name}</span>;
}

export function Farewell({ name }: { name: string }) {
  return <span>bye, {name}</span>;
}
