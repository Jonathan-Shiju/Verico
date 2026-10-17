import Link from "next/link";
import { notFound } from "next/navigation";

import { listPosts, readPost } from "../posts";

export function generateStaticParams() {
  return listPosts().map((post) => ({ slug: post.slug }));
}

export async function generateMetadata({ params }) {
  const { slug } = await params;
  const post = readPost(slug);
  return { title: post?.title ? `${post.title} · Verico` : "Blog · Verico" };
}

export default async function BlogPost({ params }) {
  const { slug } = await params;
  const post = readPost(slug);
  if (!post || !post.title) notFound();
  return (
    <main className="sheet blog">
      <p className="form-id">
        <Link href="/blog">Blog</Link>
      </p>
      <h1>{post.title}</h1>
      {post.date ? <time dateTime={post.date}>{post.date}</time> : null}
      <article dangerouslySetInnerHTML={{ __html: post.html }} />
    </main>
  );
}
