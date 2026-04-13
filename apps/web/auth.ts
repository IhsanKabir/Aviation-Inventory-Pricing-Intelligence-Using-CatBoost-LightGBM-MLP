import type { NextAuthOptions } from "next-auth";
import GoogleProvider from "next-auth/providers/google";

import { getApiBaseUrl, type AuthenticatedUser } from "@/lib/api";

type OAuthApiPayload = {
  user?: AuthenticatedUser;
  session_token?: string;
};

const authSecret = process.env.AUTH_SECRET || process.env.NEXTAUTH_SECRET;

export const googleAuthEnabled = Boolean(
  authSecret &&
  process.env.AUTH_GOOGLE_ID &&
  process.env.AUTH_GOOGLE_SECRET
);

async function createApiSession(input: {
  email?: string | null;
  fullName?: string | null;
  authProvider: string;
  providerSubject?: string | null;
}) {
  if (!input.email) {
    return null;
  }

  try {
    const response = await fetch(`${getApiBaseUrl()}/api/v1/user-auth/oauth-login`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        email: input.email,
        full_name: input.fullName,
        auth_provider: input.authProvider,
        provider_subject: input.providerSubject
      }),
      cache: "no-store"
    });

    const payload = (await response.json().catch(() => null)) as OAuthApiPayload | null;
    if (!response.ok || !payload?.session_token || !payload.user) {
      return null;
    }

    return {
      apiSessionToken: payload.session_token,
      apiUser: payload.user
    };
  } catch {
    return null;
  }
}

export const authOptions: NextAuthOptions = {
  secret: authSecret,
  session: {
    strategy: "jwt"
  },
  providers: googleAuthEnabled
    ? [
        GoogleProvider({
          clientId: process.env.AUTH_GOOGLE_ID!,
          clientSecret: process.env.AUTH_GOOGLE_SECRET!
        })
      ]
    : [],
  pages: {
    signIn: "/login"
  },
  callbacks: {
    async jwt({ token, user, account, profile }) {
      const shouldBridgeSession =
        Boolean(token.email || user?.email) &&
        (!token.apiSessionToken || account?.provider === "google");

      if (shouldBridgeSession) {
        const bridged = await createApiSession({
          email: typeof token.email === "string" ? token.email : user?.email,
          fullName: typeof token.name === "string" ? token.name : user?.name,
          authProvider: account?.provider || "google",
          providerSubject:
            typeof token.sub === "string"
              ? token.sub
              : typeof profile?.sub === "string"
                ? profile.sub
                : typeof account?.providerAccountId === "string"
                  ? account.providerAccountId
                  : null
        });

        if (bridged) {
          token.apiSessionToken = bridged.apiSessionToken;
          token.apiUser = bridged.apiUser;
        }
      }

      return token;
    },
    async session({ session, token }) {
      if (token.apiSessionToken && typeof token.apiSessionToken === "string") {
        session.apiSessionToken = token.apiSessionToken;
      }
      if (token.apiUser && typeof token.apiUser === "object") {
        session.apiUser = token.apiUser as AuthenticatedUser;
      } else if (session.user?.email) {
        session.apiUser = {
          user_id: "",
          email: session.user.email,
          full_name: session.user.name
        };
      }
      return session;
    }
  }
};
