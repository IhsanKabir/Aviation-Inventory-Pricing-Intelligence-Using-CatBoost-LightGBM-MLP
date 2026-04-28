import type { DefaultSession } from "next-auth";

import type { AuthenticatedUser } from "@/lib/api";

declare module "next-auth" {
  interface Session {
    apiSessionToken?: string;
    apiUser?: AuthenticatedUser | null;
    user?: DefaultSession["user"];
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    apiSessionToken?: string;
    apiUser?: AuthenticatedUser | null;
  }
}
