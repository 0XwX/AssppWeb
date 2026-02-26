import { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import PageContainer from '../Layout/PageContainer';
import Modal from '../common/Modal';
import { check401 } from '../../api/client';
import { useAccountsStore } from '../../store/accounts';
import { useAuthStore } from '../../store/auth';
import { useToastStore } from '../../store/toast';
import { encryptData, decryptData } from '../../utils/crypto';
import { countryCodeMap } from '../../apple/config';
import type { Account } from '../../types';

interface ServerInfo {
  buildCommit?: string;
  buildDate?: string;
  autoCleanupDays?: number;
  autoCleanupMaxMB?: number;
  storageSizeMB?: number;
  storageFileCount?: number;
  r2CdnDomain?: string;
}

const entityTypes = [
  { value: 'software', label: 'iPhone' },
  { value: 'iPadSoftware', label: 'iPad' },
];

export default function SettingsPage() {
  const { t, i18n } = useTranslation();
  const { accounts, addAccount, updateAccount } = useAccountsStore();
  const { required, solving, logout, changePassword } = useAuthStore();
  const addToast = useToastStore((s) => s.addToast);

  const [country, setCountry] = useState(
    () => localStorage.getItem('asspp-default-country') || 'US',
  );
  const [entity, setEntity] = useState(
    () => localStorage.getItem('asspp-default-entity') || 'software',
  );
  const [serverInfo, setServerInfo] = useState<ServerInfo | null>(null);

  const [exportModalOpen, setExportModalOpen] = useState(false);
  const [exportPassword, setExportPassword] = useState('');
  const [exportConfirmPassword, setExportConfirmPassword] = useState('');

  const [importModalOpen, setImportModalOpen] = useState(false);
  const [importPassword, setImportPassword] = useState('');
  const [importFileData, setImportFileData] = useState('');

  const [conflictModalOpen, setConflictModalOpen] = useState(false);
  const [pendingAccounts, setPendingAccounts] = useState<Account[]>([]);
  const [conflictStats, setConflictStats] = useState({ conflict: 0, new: 0 });

  const [changePasswordModalOpen, setChangePasswordModalOpen] = useState(false);
  const [currentPwd, setCurrentPwd] = useState('');
  const [newPwd, setNewPwd] = useState('');
  const [confirmNewPwd, setConfirmNewPwd] = useState('');
  const [changePwdLoading, setChangePwdLoading] = useState(false);

  const [editCleanupDays, setEditCleanupDays] = useState('');
  const [editCleanupMaxMB, setEditCleanupMaxMB] = useState('');
  const [savingCleanup, setSavingCleanup] = useState(false);

  useEffect(() => {
    localStorage.setItem('asspp-default-country', country);
  }, [country]);

  useEffect(() => {
    localStorage.setItem('asspp-default-entity', entity);
  }, [entity]);

  useEffect(() => {
    fetch('/api/settings')
      .then(check401)
      .then((r) => (r.ok ? r.json() : null))
      .then((info: ServerInfo | null) => {
        setServerInfo(info);
        if (info) {
          setEditCleanupDays(String(info.autoCleanupDays ?? 0));
          setEditCleanupMaxMB(String(info.autoCleanupMaxMB ?? 0));
        }
      })
      .catch(() => setServerInfo(null));
  }, []);

  const sortedCountries = Object.keys(countryCodeMap).sort((a, b) =>
    t(`countries.${a}`, a).localeCompare(t(`countries.${b}`, b)),
  );

  const handleExport = async () => {
    if (exportPassword !== exportConfirmPassword) {
      addToast(t('settings.data.passwordMismatch'), 'error');
      return;
    }
    try {
      const encrypted = await encryptData(accounts, exportPassword);
      const blob = new Blob([encrypted], { type: 'text/plain' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'asspp-accounts.enc';
      a.click();
      URL.revokeObjectURL(url);

      setExportModalOpen(false);
      setExportPassword('');
      setExportConfirmPassword('');
      addToast(t('settings.data.exportSuccess'), 'success');
    } catch {
      addToast(t('settings.data.exportFailed'), 'error');
    }
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (event) => {
      const content = event.target?.result as string;
      setImportFileData(content);
      setImportModalOpen(true);
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const handleImport = async () => {
    try {
      const parsed = await decryptData(importFileData, importPassword);
      if (!Array.isArray(parsed)) throw new Error('Invalid format');
      const valid = parsed.filter(
        (item: unknown) =>
          item &&
          typeof item === 'object' &&
          'email' in item &&
          typeof (item as Record<string, unknown>).email === 'string' &&
          ((item as Record<string, unknown>).email as string).length > 0,
      ) as Account[];
      if (valid.length === 0) throw new Error('No valid accounts found');

      if (accounts.length === 0) {
        for (const acc of valid) {
          await addAccount(acc);
        }
        addToast(t('settings.data.importSuccess'), 'success');
        setImportModalOpen(false);
        setImportPassword('');
      } else {
        let conflictCount = 0;
        let newCount = 0;
        valid.forEach((imported) => {
          if (accounts.some((a) => a.email === imported.email)) conflictCount++;
          else newCount++;
        });

        if (conflictCount > 0) {
          setConflictStats({ conflict: conflictCount, new: newCount });
          setPendingAccounts(valid);
          setImportModalOpen(false);
          setImportPassword('');
          setConflictModalOpen(true);
        } else {
          for (const acc of valid) {
            await addAccount(acc);
          }
          addToast(t('settings.data.importSuccess'), 'success');
          setImportModalOpen(false);
          setImportPassword('');
        }
      }
    } catch {
      addToast(t('settings.data.incorrectPassword'), 'error');
    }
  };

  const handleResolveConflict = async (overwrite: boolean) => {
    for (const imported of pendingAccounts) {
      const exists = accounts.some((a) => a.email === imported.email);
      if (exists) {
        if (overwrite) await updateAccount(imported);
      } else {
        await addAccount(imported);
      }
    }
    setConflictModalOpen(false);
    setPendingAccounts([]);
    addToast(t('settings.data.importSuccess'), 'success');
  };

  const handleChangePassword = async () => {
    if (newPwd !== confirmNewPwd) {
      addToast(t('auth.passwordMismatch'), 'error');
      return;
    }
    setChangePwdLoading(true);
    const result = await changePassword(currentPwd, newPwd);
    setChangePwdLoading(false);
    if (result.ok) {
      addToast(t('auth.passwordChanged'), 'success');
      setChangePasswordModalOpen(false);
      setCurrentPwd('');
      setNewPwd('');
      setConfirmNewPwd('');
    } else if (result.error === 'incorrect') {
      addToast(t('auth.invalidPassword'), 'error');
    } else {
      addToast(t('auth.changeFailed'), 'error');
    }
  };

  const handleSaveCleanup = async () => {
    setSavingCleanup(true);
    try {
      const res = await fetch('/api/settings', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          autoCleanupDays: parseInt(editCleanupDays) || 0,
          autoCleanupMaxMB: parseInt(editCleanupMaxMB) || 0,
        }),
      });
      check401(res);
      if (res.status === 401) return;
      if (res.ok) {
        const updated = (await res.json()) as {
          autoCleanupDays: number;
          autoCleanupMaxMB: number;
        };
        setServerInfo((prev) => (prev ? { ...prev, ...updated } : prev));
        addToast(t('settings.server.saved'), 'success');
      } else {
        addToast(t('settings.server.saveFailed'), 'error');
      }
    } catch {
      addToast(t('settings.server.saveFailed'), 'error');
    }
    setSavingCleanup(false);
  };

  const cleanupChanged =
    serverInfo &&
    (parseInt(editCleanupDays) !== (serverInfo.autoCleanupDays ?? 0) ||
      parseInt(editCleanupMaxMB) !== (serverInfo.autoCleanupMaxMB ?? 0));

  const handleLogout = async () => {
    await logout();
  };

  return (
    <PageContainer title={t('settings.title')}>
      <div className="space-y-6">
        <section className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 p-6">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            {t('settings.language.title')}
          </h2>
          <div className="space-y-4">
            <div>
              <label
                htmlFor="language"
                className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
              >
                {t('settings.language.label')}
              </label>
              <select
                id="language"
                value={i18n.resolvedLanguage || 'en-US'}
                onChange={async (e) => {
                  const newLang = e.target.value;
                  await i18n.changeLanguage(newLang);
                  addToast(t('settings.language.changed'), 'success');
                }}
                className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
              >
                <option value="en-US">English (US)</option>
                <option value="zh-CN">简体中文</option>
                <option value="zh-TW">繁體中文</option>
                <option value="ja">日本語</option>
                <option value="ko">한국어</option>
                <option value="ru">Русский</option>
              </select>
            </div>
          </div>
        </section>

        <section className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 p-6">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            {t('settings.defaults.title')}
          </h2>
          <div className="space-y-4">
            <div>
              <label
                htmlFor="country"
                className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
              >
                {t('settings.defaults.country')}
              </label>
              <select
                id="country"
                value={country}
                onChange={(e) => {
                  setCountry(e.target.value);
                  addToast(t('settings.defaults.countryChanged'), 'success');
                }}
                className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
              >
                {sortedCountries.map((code) => (
                  <option key={code} value={code}>
                    {t(`countries.${code}`, code)} ({code})
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label
                htmlFor="entity"
                className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
              >
                {t('settings.defaults.entity')}
              </label>
              <select
                id="entity"
                value={entity}
                onChange={(e) => {
                  setEntity(e.target.value);
                  addToast(t('settings.defaults.entityChanged'), 'success');
                }}
                className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
              >
                {entityTypes.map((et) => (
                  <option key={et.value} value={et.value}>
                    {et.label}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </section>

        <section className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 p-6">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            {t('settings.server.title')}
          </h2>
          {serverInfo ? (
            <dl className="space-y-3">
              {required && (
                <div>
                  <dt className="text-sm font-medium text-gray-500 dark:text-gray-400">
                    {t('settings.server.accessPassword')}
                  </dt>
                  <dd className="flex items-center gap-2 mt-1.5">
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">
                      {t('settings.server.enabled')}
                    </span>
                    <button
                      onClick={() => setChangePasswordModalOpen(true)}
                      className="px-3 py-1 text-xs font-medium text-blue-600 dark:text-blue-400 border border-blue-300 dark:border-blue-800 rounded-md hover:bg-blue-50 dark:hover:bg-blue-900/30 transition-colors"
                    >
                      {t('auth.changePasswordBtn')}
                    </button>
                    <button
                      onClick={handleLogout}
                      className="px-3 py-1 text-xs font-medium text-red-600 dark:text-red-400 border border-red-300 dark:border-red-800 rounded-md hover:bg-red-50 dark:hover:bg-red-900/30 transition-colors"
                    >
                      {t('auth.logout')}
                    </button>
                  </dd>
                </div>
              )}
              <div>
                <dt className="text-sm font-medium text-gray-500 dark:text-gray-400">
                  {t('settings.server.storage')}
                </dt>
                <dd className="text-sm text-gray-900 dark:text-white mt-1">
                  {serverInfo.storageFileCount != null ? (
                    <>
                      <span className="font-mono">
                        {serverInfo.storageSizeMB != null && serverInfo.storageSizeMB >= 1024
                          ? `${(serverInfo.storageSizeMB / 1024).toFixed(2)} GB`
                          : `${serverInfo.storageSizeMB ?? 0} MB`}
                      </span>
                      <span className="text-gray-500 dark:text-gray-400 ml-2 text-xs">
                        {serverInfo.storageFileCount > 0
                          ? t('settings.server.storageFiles', {
                              count: serverInfo.storageFileCount,
                            })
                          : t('settings.server.storageEmpty')}
                      </span>
                    </>
                  ) : (
                    <span className="text-gray-400 dark:text-gray-500">—</span>
                  )}
                </dd>
              </div>
              <div>
                <dt className="text-sm font-medium text-gray-500 dark:text-gray-400">
                  {t('settings.server.cdnDomain')}
                </dt>
                <dd className="text-sm mt-1">
                  {serverInfo.r2CdnDomain ? (
                    <span className="inline-flex items-center gap-2">
                      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">
                        {t('settings.server.enabled')}
                      </span>
                      <span className="font-mono text-gray-900 dark:text-white">
                        {serverInfo.r2CdnDomain}
                      </span>
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-2">
                      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400">
                        {t('settings.server.disabled')}
                      </span>
                      <span className="text-xs text-gray-400 dark:text-gray-500">
                        {t('settings.server.cdnDomainHint')}
                      </span>
                    </span>
                  )}
                </dd>
              </div>
              <div>
                <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-1">
                  {t('settings.server.cleanupDays')}
                </dt>
                <dd>
                  <input
                    type="number"
                    min="0"
                    value={editCleanupDays}
                    onChange={(e) => setEditCleanupDays(e.target.value)}
                    className="w-32 rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm text-gray-900 dark:text-white font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
                  />
                  <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
                    {t('settings.server.cleanupDaysHint')}
                  </p>
                </dd>
              </div>
              <div>
                <dt className="text-sm font-medium text-gray-500 dark:text-gray-400 mb-1">
                  {t('settings.server.cleanupMaxMB')}
                </dt>
                <dd>
                  <input
                    type="number"
                    min="0"
                    value={editCleanupMaxMB}
                    onChange={(e) => setEditCleanupMaxMB(e.target.value)}
                    className="w-32 rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-1.5 text-sm text-gray-900 dark:text-white font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
                  />
                  <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">
                    {t('settings.server.cleanupMaxMBHint')}
                  </p>
                </dd>
              </div>
              {cleanupChanged && (
                <div>
                  <button
                    onClick={handleSaveCleanup}
                    disabled={savingCleanup}
                    className="px-4 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50 transition-colors"
                  >
                    {t('settings.server.saveCleanup')}
                  </button>
                </div>
              )}
            </dl>
          ) : (
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {t('settings.server.offline')}
            </p>
          )}
        </section>

        <section className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 p-6">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            {t('settings.data.title')}
          </h2>
          <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
            {t('settings.data.description')}
          </p>

          <div className="flex flex-wrap gap-3 mb-6">
            <button
              onClick={() => setExportModalOpen(true)}
              className="px-4 py-2 text-sm font-medium text-blue-600 dark:text-blue-400 border border-blue-300 dark:border-blue-800 rounded-lg hover:bg-blue-50 dark:hover:bg-blue-900/30 transition-colors"
            >
              {t('settings.data.exportBtn')}
            </button>
            <label className="px-4 py-2 text-sm font-medium text-green-600 dark:text-green-400 border border-green-300 dark:border-green-800 rounded-lg hover:bg-green-50 dark:hover:bg-green-900/30 transition-colors cursor-pointer">
              {t('settings.data.importBtn')}
              <input type="file" className="hidden" accept=".enc" onChange={handleFileSelect} />
            </label>
          </div>

          <button
            onClick={() => {
              if (!confirm(t('settings.data.confirm'))) return;
              localStorage.clear();
              indexedDB.deleteDatabase('asspp-accounts');
              addToast(t('settings.data.cleared'), 'success');
              setTimeout(() => {
                window.location.href = '/';
              }, 1000);
            }}
            className="px-4 py-2 text-sm font-medium text-red-600 dark:text-red-400 border border-red-300 dark:border-red-800 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/30 transition-colors"
          >
            {t('settings.data.button')}
          </button>
        </section>

        <section className="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 p-6">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            {t('settings.about.title')}
          </h2>
          <p className="text-sm text-gray-600 dark:text-gray-400">
            {t('settings.about.description')}
          </p>
          {serverInfo && (
            <dl className="mt-3 space-y-2">
              {serverInfo.buildCommit && serverInfo.buildCommit !== 'unknown' && (
                <div>
                  <dt className="text-xs font-medium text-gray-400 dark:text-gray-500">
                    {t('settings.about.buildCommit')}
                  </dt>
                  <dd className="text-xs text-gray-500 dark:text-gray-400 font-mono">
                    {serverInfo.buildCommit.slice(0, 7)}
                  </dd>
                </div>
              )}
              {serverInfo.buildDate && serverInfo.buildDate !== 'unknown' && (
                <div>
                  <dt className="text-xs font-medium text-gray-400 dark:text-gray-500">
                    {t('settings.about.buildDate')}
                  </dt>
                  <dd className="text-xs text-gray-500 dark:text-gray-400">
                    {new Date(serverInfo.buildDate).toLocaleString()}
                  </dd>
                </div>
              )}
            </dl>
          )}
        </section>
      </div>

      <Modal
        open={exportModalOpen}
        onClose={() => setExportModalOpen(false)}
        title={t('settings.data.exportBtn')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              {t('settings.data.passwordPrompt')}
            </label>
            <input
              type="password"
              value={exportPassword}
              onChange={(e) => setExportPassword(e.target.value)}
              className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              {t('settings.data.passwordConfirm')}
            </label>
            <input
              type="password"
              value={exportConfirmPassword}
              onChange={(e) => setExportConfirmPassword(e.target.value)}
              className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
            />
          </div>
        </div>
        <div className="mt-6 flex justify-end gap-3">
          <button
            onClick={() => setExportModalOpen(false)}
            className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            {t('settings.data.cancel')}
          </button>
          <button
            onClick={handleExport}
            disabled={!exportPassword || !exportConfirmPassword}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {t('settings.data.confirmBtn')}
          </button>
        </div>
      </Modal>

      <Modal
        open={importModalOpen}
        onClose={() => setImportModalOpen(false)}
        title={t('settings.data.importBtn')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              {t('settings.data.passwordPrompt')}
            </label>
            <input
              type="password"
              value={importPassword}
              onChange={(e) => setImportPassword(e.target.value)}
              className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
            />
          </div>
        </div>
        <div className="mt-6 flex justify-end gap-3">
          <button
            onClick={() => setImportModalOpen(false)}
            className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            {t('settings.data.cancel')}
          </button>
          <button
            onClick={handleImport}
            disabled={!importPassword}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {t('settings.data.confirmBtn')}
          </button>
        </div>
      </Modal>

      <Modal
        open={conflictModalOpen}
        onClose={() => setConflictModalOpen(false)}
        title={t('settings.data.conflictTitle')}
      >
        <p className="text-sm text-gray-700 dark:text-gray-300 mb-6">
          {t('settings.data.conflictDesc', {
            conflict: conflictStats.conflict,
            new: conflictStats.new,
          })}
        </p>
        <div className="flex flex-col gap-3">
          <button
            onClick={() => handleResolveConflict(true)}
            className="w-full px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700 transition-colors"
          >
            {t('settings.data.conflictOverwrite')}
          </button>
          <button
            onClick={() => handleResolveConflict(false)}
            className="w-full px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            {t('settings.data.conflictSkip')}
          </button>
          <button
            onClick={() => setConflictModalOpen(false)}
            className="w-full px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 mt-2 transition-colors"
          >
            {t('settings.data.cancel')}
          </button>
        </div>
      </Modal>

      <Modal
        open={changePasswordModalOpen}
        onClose={() => setChangePasswordModalOpen(false)}
        title={t('auth.changePasswordTitle')}
      >
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              {t('auth.currentPassword')}
            </label>
            <input
              type="password"
              value={currentPwd}
              onChange={(e) => setCurrentPwd(e.target.value)}
              className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              {t('auth.newPassword')}
            </label>
            <input
              type="password"
              value={newPwd}
              onChange={(e) => setNewPwd(e.target.value)}
              className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              {t('auth.confirmPassword')}
            </label>
            <input
              type="password"
              value={confirmNewPwd}
              onChange={(e) => setConfirmNewPwd(e.target.value)}
              className="block w-full rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
            />
          </div>
        </div>
        <div className="mt-6 flex justify-end gap-3">
          <button
            onClick={() => setChangePasswordModalOpen(false)}
            className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            {t('settings.data.cancel')}
          </button>
          <button
            onClick={handleChangePassword}
            disabled={!currentPwd || !newPwd || !confirmNewPwd || changePwdLoading}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {solving ? t('auth.solving') : t('settings.data.confirmBtn')}
          </button>
        </div>
      </Modal>
    </PageContainer>
  );
}
