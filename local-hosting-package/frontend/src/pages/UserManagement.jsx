import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from "@/components/ui/dialog";
import { toast } from "sonner";
import { Users, UserPlus, Edit, Trash2, Key, Shield, RefreshCw } from "lucide-react";
import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();

const ROLES = [
  { value: "admin", label: "Admin", color: "bg-red-100 text-red-700" },
  { value: "state_officer", label: "State Officer", color: "bg-blue-100 text-blue-700" },
  { value: "district_officer", label: "District Officer", color: "bg-purple-100 text-purple-700" },
  { value: "viewer", label: "Viewer", color: "bg-slate-100 text-slate-700" }
];

const UserManagement = () => {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [showEditDialog, setShowEditDialog] = useState(false);
  const [selectedUser, setSelectedUser] = useState(null);
  const [formData, setFormData] = useState({
    email: "",
    full_name: "",
    password: "",
    role: "viewer",
    district_code: "",
    is_active: true
  });

  const fetchUsers = useCallback(async () => {
    try {
      const response = await axios.get(`${BACKEND_URL}/api/auth/users`);
      setUsers(response.data);
    } catch (error) {
      toast.error("Failed to fetch users");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchUsers();
  }, [fetchUsers]);

  const handleCreate = async () => {
    try {
      await axios.post(`${BACKEND_URL}/api/auth/users`, formData);
      toast.success("User created successfully");
      setShowCreateDialog(false);
      setFormData({ email: "", full_name: "", password: "", role: "viewer", district_code: "", is_active: true });
      fetchUsers();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to create user");
    }
  };

  const handleUpdate = async () => {
    try {
      const updateData = {
        full_name: formData.full_name,
        role: formData.role,
        district_code: formData.district_code || null,
        is_active: formData.is_active
      };
      await axios.put(`${BACKEND_URL}/api/auth/users/${selectedUser.id}`, updateData);
      toast.success("User updated successfully");
      setShowEditDialog(false);
      fetchUsers();
    } catch (error) {
      toast.error("Failed to update user");
    }
  };

  const handleDelete = async (userId) => {
    if (!window.confirm("Are you sure you want to delete this user?")) return;
    
    try {
      await axios.delete(`${BACKEND_URL}/api/auth/users/${userId}`);
      toast.success("User deleted successfully");
      fetchUsers();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to delete user");
    }
  };

  const handleResetPassword = async (userId) => {
    try {
      const response = await axios.post(`${BACKEND_URL}/api/auth/users/${userId}/reset-password`);
      toast.success(`Password reset. Temporary password: ${response.data.temporary_password}`);
    } catch (error) {
      toast.error("Failed to reset password");
    }
  };

  const openEditDialog = (user) => {
    setSelectedUser(user);
    setFormData({
      email: user.email,
      full_name: user.full_name,
      role: user.role,
      district_code: user.district_code || "",
      is_active: user.is_active
    });
    setShowEditDialog(true);
  };

  const getRoleBadge = (role) => {
    const roleInfo = ROLES.find(r => r.value === role) || ROLES[3];
    return <Badge className={roleInfo.color}>{roleInfo.label}</Badge>;
  };

  if (loading) {
    return <div className="flex items-center justify-center h-96"><div className="loading-spinner" /></div>;
  }

  return (
    <div className="space-y-6" data-testid="user-management">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-slate-900" style={{ fontFamily: 'Manrope' }}>User Management</h1>
          <p className="text-slate-500 mt-1">Manage user accounts and access permissions</p>
        </div>
        <div className="flex gap-3">
          <Button variant="outline" size="sm" onClick={fetchUsers}>
            <RefreshCw className="w-4 h-4 mr-2" />Refresh
          </Button>
          <Dialog open={showCreateDialog} onOpenChange={setShowCreateDialog}>
            <DialogTrigger asChild>
              <Button data-testid="create-user-btn">
                <UserPlus className="w-4 h-4 mr-2" />Add User
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Create New User</DialogTitle>
              </DialogHeader>
              <div className="space-y-4 py-4">
                <div className="space-y-2">
                  <Label>Full Name</Label>
                  <Input
                    value={formData.full_name}
                    onChange={(e) => setFormData({...formData, full_name: e.target.value})}
                    placeholder="Enter full name"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Email</Label>
                  <Input
                    type="email"
                    value={formData.email}
                    onChange={(e) => setFormData({...formData, email: e.target.value})}
                    placeholder="user@example.com"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Password</Label>
                  <Input
                    type="password"
                    value={formData.password}
                    onChange={(e) => setFormData({...formData, password: e.target.value})}
                    placeholder="Min 6 characters"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Role</Label>
                  <Select value={formData.role} onValueChange={(v) => setFormData({...formData, role: v})}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {ROLES.map(role => (
                        <SelectItem key={role.value} value={role.value}>{role.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                {formData.role === "district_officer" && (
                  <div className="space-y-2">
                    <Label>District Code</Label>
                    <Input
                      value={formData.district_code}
                      onChange={(e) => setFormData({...formData, district_code: e.target.value})}
                      placeholder="e.g., PUNE"
                    />
                  </div>
                )}
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setShowCreateDialog(false)}>Cancel</Button>
                <Button onClick={handleCreate}>Create User</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <Card className="border-slate-200">
        <CardHeader className="pb-3">
          <CardTitle className="text-lg flex items-center gap-2">
            <Users className="w-5 h-5" />
            Users ({users.length})
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow className="bg-slate-50">
                <TableHead>Name</TableHead>
                <TableHead>Email</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>District</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {users.map((user) => (
                <TableRow key={user.id}>
                  <TableCell className="font-medium">{user.full_name}</TableCell>
                  <TableCell>{user.email}</TableCell>
                  <TableCell>{getRoleBadge(user.role)}</TableCell>
                  <TableCell>{user.district_code || "-"}</TableCell>
                  <TableCell>
                    <Badge className={user.is_active ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>
                      {user.is_active ? "Active" : "Inactive"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-slate-500 text-sm">
                    {user.created_at ? new Date(user.created_at).toLocaleDateString() : "-"}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-1">
                      <Button variant="ghost" size="sm" onClick={() => openEditDialog(user)} title="Edit">
                        <Edit className="w-4 h-4" />
                      </Button>
                      <Button variant="ghost" size="sm" onClick={() => handleResetPassword(user.id)} title="Reset Password">
                        <Key className="w-4 h-4" />
                      </Button>
                      <Button variant="ghost" size="sm" onClick={() => handleDelete(user.id)} title="Delete" className="text-red-500 hover:text-red-700">
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Edit Dialog */}
      <Dialog open={showEditDialog} onOpenChange={setShowEditDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit User</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label>Full Name</Label>
              <Input
                value={formData.full_name}
                onChange={(e) => setFormData({...formData, full_name: e.target.value})}
              />
            </div>
            <div className="space-y-2">
              <Label>Email</Label>
              <Input value={formData.email} disabled className="bg-slate-100" />
            </div>
            <div className="space-y-2">
              <Label>Role</Label>
              <Select value={formData.role} onValueChange={(v) => setFormData({...formData, role: v})}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {ROLES.map(role => (
                    <SelectItem key={role.value} value={role.value}>{role.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {formData.role === "district_officer" && (
              <div className="space-y-2">
                <Label>District Code</Label>
                <Input
                  value={formData.district_code}
                  onChange={(e) => setFormData({...formData, district_code: e.target.value})}
                />
              </div>
            )}
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="is_active"
                checked={formData.is_active}
                onChange={(e) => setFormData({...formData, is_active: e.target.checked})}
                className="rounded"
              />
              <Label htmlFor="is_active">Active</Label>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowEditDialog(false)}>Cancel</Button>
            <Button onClick={handleUpdate}>Save Changes</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Role Permissions Info */}
      <Card className="border-slate-200">
        <CardHeader className="pb-3">
          <CardTitle className="text-lg flex items-center gap-2">
            <Shield className="w-5 h-5" />
            Role Permissions
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow className="bg-slate-50">
                <TableHead>Role</TableHead>
                <TableHead className="text-center">View All</TableHead>
                <TableHead className="text-center">Export</TableHead>
                <TableHead className="text-center">Manage Users</TableHead>
                <TableHead className="text-center">Import Data</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TableRow>
                <TableCell>{getRoleBadge("admin")}</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>{getRoleBadge("state_officer")}</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>{getRoleBadge("district_officer")}</TableCell>
                <TableCell className="text-center text-amber-500">District Only</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>{getRoleBadge("viewer")}</TableCell>
                <TableCell className="text-center text-emerald-500">✓</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
                <TableCell className="text-center text-red-500">✗</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
};

export default UserManagement;
