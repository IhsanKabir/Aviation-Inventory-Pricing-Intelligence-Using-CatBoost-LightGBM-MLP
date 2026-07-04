import { redirect } from "next/navigation";

import { googleAuthEnabled } from "@/auth";
import { UserAuthForm } from "@/components/user-auth-form";
import { sanitizeReturnPath } from "@/lib/navigation";
import { getCurrentUserSession } from "@/lib/user-auth";

type LoginPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const params = (await searchParams) ?? {};
  const nextParam = params.next;
  const next = sanitizeReturnPath(
    Array.isArray(nextParam) ? nextParam[0] : nextParam,
    "/routes"
  );

  const { user } = await getCurrentUserSession();
  // Only redirect a genuinely-valid session (getCurrentUserSession validates the
  // token now), and honor ?next so signing in from the discount page returns
  // there instead of always dumping the user on /routes.
  if (user) {
    redirect(next);
  }

  return (
    <div className="stack" style={{ maxWidth: "34rem", margin: "0 auto" }}>
      <UserAuthForm googleEnabled={googleAuthEnabled} />
    </div>
  );
}
