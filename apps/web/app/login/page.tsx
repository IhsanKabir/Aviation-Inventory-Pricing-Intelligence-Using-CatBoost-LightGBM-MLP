import { redirect } from "next/navigation";

import { UserAuthForm } from "@/components/user-auth-form";
import { getCurrentUserSession } from "@/lib/user-auth";

export default async function LoginPage() {
  const { user } = await getCurrentUserSession();
  if (user) {
    redirect("/routes");
  }
  return (
    <div className="stack" style={{ maxWidth: "34rem", margin: "0 auto" }}>
      <UserAuthForm googleEnabled={Boolean(process.env.AUTH_GOOGLE_ID && process.env.AUTH_GOOGLE_SECRET)} />
    </div>
  );
}
