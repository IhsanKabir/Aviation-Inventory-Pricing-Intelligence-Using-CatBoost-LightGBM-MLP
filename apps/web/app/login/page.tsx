import { redirect } from "next/navigation";

import { googleAuthEnabled } from "@/auth";
import { UserAuthForm } from "@/components/user-auth-form";
import { getCurrentUserSession } from "@/lib/user-auth";

export default async function LoginPage() {
  const { user } = await getCurrentUserSession();
  if (user) {
    redirect("/routes");
  }
  return (
    <div className="stack" style={{ maxWidth: "34rem", margin: "0 auto" }}>
      <UserAuthForm googleEnabled={googleAuthEnabled} />
    </div>
  );
}
