import Link from "next/link";

import { listPosts } from "./posts";

export const metadata = {
  title: "Blog · Verico",
};

export default function BlogPage() {
  const posts = listPosts();
  return (
    <main className="sheet blog">
      <p className="form-id">VRC-04</p>
      <h1>Blog</h1>
      {posts.length ? (
        <ul className="blog-index">
          {posts.map((post) => (
            <li key={post.slug}>
              <Link href={`/blog/${post.slug}`}>{post.title}</Link>
              {post.date ? <time dateTime={post.date}>{post.date}</time> : null}
            </li>
          ))}
        </ul>
      ) : (
        <p>No posts yet. A post is a markdown file in <code>content/blog</code> with a title.</p>
      )}
    </main>
  );
}
