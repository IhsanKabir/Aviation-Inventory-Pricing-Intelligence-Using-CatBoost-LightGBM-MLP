import Link from "next/link";

import { SetPasswordForm } from "@/components/set-password-form";
import { getCurrentUserSession } from "@/lib/user-auth";

export const metadata = {
  title: "Account",
  description: "Manage your sign-in credentials.",
};

export default async function AccountPage() {
  const { user } = await getCurrentUserSession();

  if (!user) {
    return (
      <div className="page">
        <section className="card" style={{ display: "flex", flexDirection: "column", gap: 10, alignItems: "flex-start" }}>
          <h1>Account</h1>
          <p>Sign in to manage your account.</p>
          <Link className="button-link" href="/login">Sign in</Link>
        </section>
      </div>
    );
  }

  return (
    <div className="page" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <section className="card">
        <h1>Account</h1>
        <p style={{ color: "var(--muted)" }}>{user.email}</p>
      </section>
      <section className="card">
        <h2 style={{ marginBottom: 4 }}>Desktop app password</h2>
        <p style={{ color: "var(--muted)", marginBottom: 12 }}>
          If you sign in here with Google, you don&apos;t have a password yet — the
          desktop apps (OTA Discount Report, IATA Validator) sign in with email +
          password. Set one below; your Google sign-in keeps working unchanged.
        </p>
        <SetPasswordForm />
      </section>
    </div>
  );
}
