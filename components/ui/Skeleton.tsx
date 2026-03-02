export function Skeleton({ className = "" }: { className?: string }) {
  return <div className={`skeleton ${className}`} />;
}

export function SkeletonCard() {
  return (
    <div className="bg-surface rounded-xl border border-[var(--border)] p-5 space-y-3">
      <Skeleton className="h-3 w-24" />
      <Skeleton className="h-10 w-20" />
      <Skeleton className="h-3 w-16" />
    </div>
  );
}

export function SkeletonRow() {
  return (
    <div className="flex items-center gap-4 px-5 py-3.5 border-b border-[var(--border)]">
      <Skeleton className="h-3 w-32" />
      <Skeleton className="h-3 w-16 ml-auto" />
      <Skeleton className="h-3 w-16" />
      <Skeleton className="h-3 w-16" />
      <Skeleton className="h-3 w-16" />
    </div>
  );
}

export function SkeletonMap() {
  return <div className="skeleton w-full" style={{ minHeight: 360 }} />;
}
