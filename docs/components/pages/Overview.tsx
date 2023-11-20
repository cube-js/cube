import Link from "next/link";

export const Overview = () => {
  return (
    <div className="min-h-screen mx-auto container px-4 pt-16 text-center w-full flex flex-col gap-4 items-center">
      <h1 className="text-4xl font-bold">Custom Docs Overview page</h1>

      <Link href="/introduction">introduction</Link>
    </div>
  );
};
