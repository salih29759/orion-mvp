export function Skeleton({ className = "" }: { className?: string }) {
  return (
    <div
      className={`animate-pulse bg-white/5 rounded-lg ${className}`}
    />
  );
}

export function SkeletonCard() {
  return (
    <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5 space-y-3">
      <Skeleton className="h-3 w-24" />
      <Skeleton className="h-8 w-16" />
      <Skeleton className="h-3 w-32" />
    </div>
  );
}

export function SkeletonRow() {
  return (
    <div className="flex items-center gap-4 py-3 px-4">
      <Skeleton className="h-3 w-32" />
      <Skeleton className="h-3 w-20" />
      <Skeleton className="h-3 w-12" />
      <Skeleton className="h-3 w-12" />
      <Skeleton className="h-3 w-12" />
      <Skeleton className="h-3 w-16" />
    </div>
  );
}
