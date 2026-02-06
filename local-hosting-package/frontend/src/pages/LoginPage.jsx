import { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import { Eye, EyeOff, LogIn, Mail, Lock, Building2 } from "lucide-react";
import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();

const LoginPage = ({ onLogin }) => {
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const [resetMode, setResetMode] = useState(false);
  const [resetEmail, setResetEmail] = useState("");

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);

    try {
      const response = await axios.post(`${BACKEND_URL}/api/auth/login`, {
        email,
        password
      });

      const { access_token, user } = response.data;
      
      // Store token and user info
      localStorage.setItem("token", access_token);
      localStorage.setItem("user", JSON.stringify(user));
      
      // Set default axios header
      axios.defaults.headers.common["Authorization"] = `Bearer ${access_token}`;
      
      toast.success(`Welcome back, ${user.full_name}!`);
      onLogin(user);
      navigate("/executive-dashboard");
    } catch (error) {
      const message = error.response?.data?.detail || "Login failed. Please check your credentials.";
      toast.error(message);
    } finally {
      setLoading(false);
    }
  };

  const handleResetPassword = async (e) => {
    e.preventDefault();
    setLoading(true);

    try {
      await axios.post(`${BACKEND_URL}/api/auth/password-reset-request`, {
        email: resetEmail
      });
      toast.success("Password reset instructions sent to your email");
      setResetMode(false);
    } catch (error) {
      toast.error("Failed to send reset email");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo/Header */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-blue-500 to-purple-600 mb-4">
            <Building2 className="w-8 h-8 text-white" />
          </div>
          <h1 className="text-3xl font-bold text-white" style={{ fontFamily: 'Manrope' }}>
            MahaEduMe
          </h1>
          <p className="text-slate-400 mt-2">Maharashtra Education Dashboard</p>
        </div>

        <Card className="border-slate-700 bg-slate-800/50 backdrop-blur">
          <CardHeader className="pb-4">
            <CardTitle className="text-xl text-white">
              {resetMode ? "Reset Password" : "Sign In"}
            </CardTitle>
            <CardDescription className="text-slate-400">
              {resetMode 
                ? "Enter your email to receive reset instructions" 
                : "Enter your credentials to access the dashboard"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {resetMode ? (
              <form onSubmit={handleResetPassword} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="reset-email" className="text-slate-300">Email</Label>
                  <div className="relative">
                    <Mail className="absolute left-3 top-3 h-4 w-4 text-slate-400" />
                    <Input
                      id="reset-email"
                      type="email"
                      placeholder="your.email@example.com"
                      value={resetEmail}
                      onChange={(e) => setResetEmail(e.target.value)}
                      className="pl-10 bg-slate-700 border-slate-600 text-white placeholder:text-slate-400"
                      required
                    />
                  </div>
                </div>
                
                <Button type="submit" className="w-full bg-blue-600 hover:bg-blue-700" disabled={loading}>
                  {loading ? "Sending..." : "Send Reset Link"}
                </Button>
                
                <Button 
                  type="button" 
                  variant="ghost" 
                  className="w-full text-slate-400 hover:text-white"
                  onClick={() => setResetMode(false)}
                >
                  Back to Login
                </Button>
              </form>
            ) : (
              <form onSubmit={handleLogin} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="email" className="text-slate-300">Email</Label>
                  <div className="relative">
                    <Mail className="absolute left-3 top-3 h-4 w-4 text-slate-400" />
                    <Input
                      id="email"
                      type="email"
                      placeholder="your.email@example.com"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      className="pl-10 bg-slate-700 border-slate-600 text-white placeholder:text-slate-400"
                      required
                      data-testid="login-email"
                    />
                  </div>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="password" className="text-slate-300">Password</Label>
                  <div className="relative">
                    <Lock className="absolute left-3 top-3 h-4 w-4 text-slate-400" />
                    <Input
                      id="password"
                      type={showPassword ? "text" : "password"}
                      placeholder="••••••••"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="pl-10 pr-10 bg-slate-700 border-slate-600 text-white placeholder:text-slate-400"
                      required
                      data-testid="login-password"
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-3 text-slate-400 hover:text-white"
                    >
                      {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                    </button>
                  </div>
                </div>
                
                <div className="flex items-center justify-end">
                  <button
                    type="button"
                    onClick={() => setResetMode(true)}
                    className="text-sm text-blue-400 hover:text-blue-300"
                  >
                    Forgot password?
                  </button>
                </div>
                
                <Button 
                  type="submit" 
                  className="w-full bg-blue-600 hover:bg-blue-700" 
                  disabled={loading}
                  data-testid="login-submit"
                >
                  {loading ? (
                    "Signing in..."
                  ) : (
                    <>
                      <LogIn className="w-4 h-4 mr-2" />
                      Sign In
                    </>
                  )}
                </Button>
              </form>
            )}
            
            <div className="mt-6 pt-6 border-t border-slate-700">
              <p className="text-center text-sm text-slate-400">
                Default credentials for testing:
              </p>
              <p className="text-center text-xs text-slate-500 mt-1">
                admin@mahaedume.gov.in / admin123
              </p>
            </div>
          </CardContent>
        </Card>

        <p className="text-center text-slate-500 text-sm mt-6">
          © 2025 Government of Maharashtra • Education Department
        </p>
      </div>
    </div>
  );
};

export default LoginPage;
