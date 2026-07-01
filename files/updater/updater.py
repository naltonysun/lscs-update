"""SoftUpdater — 零依赖自更新器，支持远程HTTP和本地文件夹两种模式"""
import json, os, sys, shutil, hashlib, urllib.request
from datetime import datetime

class SoftUpdater:
    """软更新器 — 从远程云文件夹或本地同步文件夹自我更新"""

    def __init__(self, base_dir: str, remote_base_url: str = "", local_base_dir: str = ""):
        self.base_dir = os.path.abspath(base_dir)
        self.remote_base_url = remote_base_url.rstrip("/")
        self.local_base_dir = os.path.abspath(local_base_dir) if local_base_dir else ""
        self.temp_updates = os.path.join(self.base_dir, "temp", "updates")
        self.temp_backup = os.path.join(self.base_dir, "temp", "backup")
        self.local_ver_path = os.path.join(self.base_dir, "data", "local_version.json")
        os.makedirs(self.temp_updates, exist_ok=True)
        os.makedirs(self.temp_backup, exist_ok=True)

    def _get_local_version(self) -> dict:
        """读取本地版本信息"""
        try:
            if os.path.exists(self.local_ver_path):
                with open(self.local_ver_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except:
            pass
        return {"version": "0.0.0", "files": {}}

    def _save_local_version(self, ver_info: dict):
        """保存本地版本信息"""
        os.makedirs(os.path.dirname(self.local_ver_path), exist_ok=True)
        with open(self.local_ver_path, "w", encoding="utf-8") as f:
            json.dump(ver_info, f, ensure_ascii=False, indent=2)

    def _fetch_remote_version(self) -> dict:
        """获取远程版本清单"""
        if self.remote_base_url:
            url = f"{self.remote_base_url}/version.json"
            req = urllib.request.Request(url, headers={"User-Agent": "updater/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        elif self.local_base_dir:
            path = os.path.join(self.local_base_dir, "version.json")
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        raise ValueError("未配置更新源")

    def _sha256(self, filepath: str) -> str:
        """计算文件的SHA256"""
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def _download_file(self, rel_path: str) -> bytes:
        """下载/复制单个文件，返回内容"""
        if self.remote_base_url:
            url = f"{self.remote_base_url}/files/{rel_path}"
            req = urllib.request.Request(url, headers={"User-Agent": "updater/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        elif self.local_base_dir:
            src = os.path.join(self.local_base_dir, "files", rel_path)
            with open(src, "rb") as f:
                return f.read()
        raise ValueError("未配置更新源")

    def check_version_only(self) -> dict:
        """仅检查版本，不下文件"""
        try:
            local_ver = self._get_local_version().get("version", "0.0.0")
            remote = self._fetch_remote_version()
            remote_ver = remote.get("version", "0.0.0")
            min_ver = remote.get("min_version", "0.0.0")
            
            if remote_ver <= local_ver:
                return {"status": "current", "local_version": local_ver}
            
            # 检查最低版本
            if local_ver < min_ver:
                return {
                    "status": "version_too_low",
                    "local_version": local_ver,
                    "remote_version": remote_ver,
                    "min_version": min_ver
                }
            
            return {
                "status": "update_available",
                "local_version": local_ver,
                "remote_version": remote_ver,
                "release_notes": remote.get("release_notes", ""),
                "files": list(remote.get("files", {}).keys()),
                "delete_files": remote.get("delete_files", [])
            }
        except Exception as e:
            return {"status": "error", "error": str(e)[:200]}

    def check_and_update(self) -> dict:
        """检查并执行更新"""
        try:
            local_info = self._get_local_version()
            local_ver = local_info.get("version", "0.0.0")
            remote = self._fetch_remote_version()
            remote_ver = remote.get("version", "0.0.0")
            
            if remote_ver <= local_ver:
                return {"status": "current", "local_version": local_ver}
            
            min_ver = remote.get("min_version", "0.0.0")
            if local_ver < min_ver:
                return {"status": "version_too_low", "local_version": local_ver, "remote_version": remote_ver, "min_version": min_ver}
            
            files_to_update = remote.get("files", {})
            delete_files = remote.get("delete_files", [])
            
            # 下载到临时目录
            for rel_path in files_to_update:
                # 路径穿越防护
                if ".." in rel_path or rel_path.startswith("/"):
                    return {"status": "error", "error": f"非法路径: {rel_path}"}
                content = self._download_file(rel_path)
                tmp_path = os.path.join(self.temp_updates, rel_path)
                os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
                with open(tmp_path, "wb") as f:
                    f.write(content)
                # 校验SHA256
                sha_info = files_to_update[rel_path].get("sha256", "")
                if sha_info and self._sha256(tmp_path) != sha_info:
                    return {"status": "error", "error": f"SHA256校验失败: {rel_path}"}
            
            # 备份旧文件 — 在备份目录下保留原始相对路径结构
            backup_info = {"from_version": local_ver, "to_version": remote_ver, "files": {}}
            for rel_path in files_to_update:
                src = os.path.join(self.base_dir, rel_path)
                if os.path.exists(src):
                    bak_path = os.path.join(self.temp_backup, rel_path + f".bak.{local_ver}")
                    os.makedirs(os.path.dirname(bak_path), exist_ok=True)
                    shutil.copy2(src, bak_path)
                    backup_info["files"][rel_path] = rel_path + f".bak.{local_ver}"
            
            # 替换文件
            for rel_path in files_to_update:
                dst = os.path.join(self.base_dir, rel_path)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                tmp_path = os.path.join(self.temp_updates, rel_path)
                shutil.copy2(tmp_path, dst)
            
            # 删除要废弃的文件
            for rel_path in delete_files:
                target = os.path.join(self.base_dir, rel_path)
                if os.path.exists(target):
                    os.remove(target)
            
            # 更新本地版本信息
            new_info = {
                "version": remote_ver,
                "files": {rel: self._sha256(os.path.join(self.base_dir, rel)) for rel in files_to_update},
                "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "previous_version": local_ver,
                "release_notes": remote.get("release_notes", "")
            }
            self._save_local_version(new_info)
            
            # 清理临时文件
            for rel_path in files_to_update:
                tmp_path = os.path.join(self.temp_updates, rel_path)
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            
            result = {"status": "updated", "from_version": local_ver, "to_version": remote_ver}
            post_action = remote.get("post_update", "")
            if post_action:
                result["post_update"] = post_action
            
            return result
        
        except Exception as e:
            return {"status": "error", "error": str(e)[:300]}

    def rollback(self) -> dict:
        """从备份回滚到上一版本"""
        try:
            local_info = self._get_local_version()
            current_ver = local_info.get("version", "0.0.0")
            
            # 递归查找所有备份文件
            if not os.path.exists(self.temp_backup):
                return {"status": "error", "error": "没有找到备份文件"}
            
            all_backups = []
            for root, dirs, files in os.walk(self.temp_backup):
                for f in files:
                    if f.endswith(f".bak.{current_ver}"):
                        all_backups.append(os.path.join(root, f))
            if not all_backups:
                for root, dirs, files in os.walk(self.temp_backup):
                    for f in files:
                        if ".bak." in f:
                            all_backups.append(os.path.join(root, f))
            
            if not all_backups:
                return {"status": "error", "error": f"没有找到版本 {current_ver} 的备份"}
            
            restored = []
            for bak_full in all_backups:
                # 从备份文件路径还原：temp/backup/data/file.json.bak.1.0.2 → data/file.json
                rel = os.path.relpath(bak_full, self.temp_backup)
                parts = rel.split(".bak.")
                if len(parts) != 2:
                    continue
                orig_rel = parts[0]
                dst = os.path.join(self.base_dir, orig_rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(bak_full, dst)
                restored.append(orig_rel)
            
            # 回滚版本号
            prev_ver = local_info.get("previous_version", "0.0.0")
            local_info["version"] = prev_ver
            local_info["previous_version"] = current_ver
            local_info["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            self._save_local_version(local_info)
            
            return {"status": "rolled_back", "from_version": current_ver, "to_version": prev_ver, "files": restored}
        
        except Exception as e:
            return {"status": "error", "error": str(e)[:300]}
