import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useShallow } from 'zustand/react/shallow';
import PageContainer from '../Layout/PageContainer';
import AppIcon from '../common/AppIcon';
import CountrySelect from '../common/CountrySelect';
import Spinner from '../common/Spinner';
import { useSearch } from '../../hooks/useSearch';
import { useTopCharts } from '../../store/topCharts';
import { useAccounts } from '../../hooks/useAccounts';
import { useDownloadAction } from '../../hooks/useDownloadAction';
import { useSettingsStore } from '../../store/settings';
import { useToastStore } from '../../store/toast';
import { countryCodeMap, storeIdToCountry } from '../../apple/config';
import { firstAccountCountry } from '../../utils/account';
import type { Software } from '../../types';

type Tab = 'search' | 'charts';

function formatFileSize(bytes: string): string {
  const b = parseInt(bytes, 10);
  if (isNaN(b)) return '';
  if (b >= 1073741824) return `${(b / 1073741824).toFixed(1)} GB`;
  if (b >= 1048576) return `${(b / 1048576).toFixed(1)} MB`;
  return `${(b / 1024).toFixed(0)} KB`;
}

function StarRating({ rating }: { rating: number }) {
  const full = Math.round(rating);
  return (
    <span className="inline-flex items-center gap-px">
      {[...Array(5)].map((_, i) => (
        <svg
          key={i}
          className={`w-3 h-3 ${i < full ? 'text-yellow-500' : 'text-gray-300 dark:text-gray-600'}`}
          viewBox="0 0 20 20"
          fill="currentColor"
        >
          <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
        </svg>
      ))}
    </span>
  );
}

export default function SearchPage() {
  const { t } = useTranslation();
  const { defaultCountry, defaultEntity } = useSettingsStore();
  const { accounts } = useAccounts();
  const initialCountry = firstAccountCountry(accounts) ?? defaultCountry;
  const addToast = useToastStore((s) => s.addToast);

  const { startDownload, toastDownloadError } = useDownloadAction();

  const [tab, setTab] = useState<Tab>('search');

  const { term, country, entity, results, loading, error, search, setSearchParam } = useSearch();
  const topCharts = useTopCharts(
    useShallow((s) => ({
      results: s.results,
      loading: s.loading,
      error: s.error,
      fetched: s.fetched,
      fetch: s.fetch,
    })),
  );

  useEffect(() => {
    if (error) addToast(error, 'error');
  }, [error, addToast]);

  useEffect(() => {
    if (topCharts.error) addToast(topCharts.error, 'error');
  }, [topCharts.error, addToast]);

  useEffect(() => {
    if (!country && initialCountry) setSearchParam({ country: initialCountry });
    if (!entity && defaultEntity) setSearchParam({ entity: defaultEntity });
  }, [country, initialCountry, entity, defaultEntity, setSearchParam]);

  const activeCountry = country || initialCountry;
  const activeEntity = entity || defaultEntity;

  const availableCountryCodes = Array.from(
    new Set(accounts.map((a) => storeIdToCountry(a.store)).filter(Boolean) as string[]),
  ).sort((a, b) => t(`countries.${a}`, a).localeCompare(t(`countries.${b}`, b)));

  const allCountryCodes = Object.keys(countryCodeMap).sort((a, b) =>
    t(`countries.${a}`, a).localeCompare(t(`countries.${b}`, b)),
  );

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!term.trim()) return;
    search(term.trim(), activeCountry, activeEntity);
  }

  const hasMatchingAccount = accounts.some((a) => storeIdToCountry(a.store) === activeCountry);

  async function handleQuickDownload(app: Software) {
    const account = accounts.find((a) => storeIdToCountry(a.store) === activeCountry);
    if (!account) {
      addToast(t('search.noMatchingAccount'), 'error');
      return;
    }
    try {
      await startDownload(account, app);
    } catch (e) {
      toastDownloadError(account, app, e);
    }
  }

  useEffect(() => {
    if (tab === 'charts' && activeCountry) {
      topCharts.fetch(activeCountry, 'top-free');
    }
  }, [tab, activeCountry, topCharts.fetch]);

  return (
    <PageContainer title={t('search.title')}>
      {/* Tabs */}
      <div className="flex border-b border-gray-200 dark:border-gray-800 mb-4">
        <button
          onClick={() => setTab('search')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            tab === 'search'
              ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          {t('search.tabs.search')}
        </button>
        <button
          onClick={() => setTab('charts')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            tab === 'charts'
              ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
          }`}
        >
          {t('search.tabs.charts')}
        </button>
      </div>

      {/* Search Tab */}
      {tab === 'search' && (
        <>
          <form onSubmit={handleSubmit} className="space-y-4 mb-6">
            <div className="flex gap-2">
              <input
                type="text"
                value={term}
                onChange={(e) => setSearchParam({ term: e.target.value })}
                placeholder={t('search.placeholder')}
                className="flex-1 rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
              />
              <button
                type="submit"
                disabled={loading || !term.trim()}
                className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors whitespace-nowrap"
              >
                {loading ? t('search.searching') : t('search.button')}
              </button>
            </div>
            <div className="flex w-full gap-3 overflow-hidden">
              <CountrySelect
                value={activeCountry}
                onChange={(c) => setSearchParam({ country: c })}
                availableCountryCodes={availableCountryCodes}
                allCountryCodes={allCountryCodes}
                className="w-1/2 truncate bg-white dark:bg-gray-800 text-gray-900 dark:text-white border-gray-300 dark:border-gray-700"
              />
              <select
                value={activeEntity}
                onChange={(e) => setSearchParam({ entity: e.target.value })}
                className="w-1/2 truncate rounded-md border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 text-base text-gray-900 dark:text-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-colors"
              >
                <option value="iPhone">iPhone</option>
                <option value="iPad">iPad</option>
              </select>
            </div>
          </form>

          {results.length === 0 && !loading && !error && (
            <div className="flex flex-col items-center justify-center py-16 px-4 bg-gray-50 dark:bg-gray-900/30 border-2 border-dashed border-gray-200 dark:border-gray-800 rounded-2xl">
              <div className="bg-white dark:bg-gray-800 p-4 rounded-full shadow-sm mb-4 border border-gray-100 dark:border-gray-700">
                <svg
                  className="w-12 h-12 text-blue-500 dark:text-blue-400"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z"
                  />
                </svg>
              </div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2 text-center">
                {t('search.empty')}
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400 text-center max-w-sm">
                {t('search.emptyDesc')}
              </p>
            </div>
          )}

          <div className="space-y-2">
            {results.map((app) => (
              <div
                key={app.id}
                className="flex items-center bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 hover:border-gray-300 dark:hover:border-gray-600 transition-colors"
              >
                <Link
                  to={`/search/${app.id}`}
                  state={{ app, country: activeCountry }}
                  className="flex items-center gap-4 flex-1 min-w-0 p-4"
                >
                  <AppIcon url={app.artworkUrl} name={app.name} size="md" />
                  <div className="flex-1 min-w-0">
                    <p className="font-medium text-gray-900 dark:text-white truncate">{app.name}</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400 truncate">
                      {app.artistName}
                    </p>
                    <div className="flex items-center gap-3 mt-1 text-xs text-gray-400 dark:text-gray-500">
                      <span>{app.formattedPrice ?? t('search.free')}</span>
                      <span>{app.primaryGenreName}</span>
                      {app.fileSizeBytes && <span>{formatFileSize(app.fileSizeBytes)}</span>}
                      <StarRating rating={app.averageUserRating} />
                    </div>
                  </div>
                </Link>
                <div className="flex shrink-0 mr-3 gap-1">
                  <Link
                    to={`/search/${app.id}/versions`}
                    state={{ app, country: activeCountry }}
                    className="p-2 text-gray-400 dark:text-gray-500 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                    title={t('search.product.versionHistory')}
                  >
                    <svg
                      className="w-5 h-5"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2}
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z"
                      />
                    </svg>
                  </Link>
                  {hasMatchingAccount && (
                    <button
                      onClick={() => handleQuickDownload(app)}
                      className="p-2 text-gray-400 dark:text-gray-500 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                      title={t('search.product.download')}
                    >
                      <svg
                        className="w-5 h-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
                        />
                      </svg>
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Charts Tab */}
      {tab === 'charts' && (
        <>
          <div className="flex w-full gap-3 overflow-hidden mb-6">
            <CountrySelect
              value={activeCountry}
              onChange={(c) => setSearchParam({ country: c })}
              availableCountryCodes={availableCountryCodes}
              allCountryCodes={allCountryCodes}
              className="w-full bg-white dark:bg-gray-800 text-gray-900 dark:text-white border-gray-300 dark:border-gray-700"
            />
          </div>

          {topCharts.loading && (
            <div className="flex justify-center py-16 text-blue-500 [&>svg]:w-8 [&>svg]:h-8">
              <Spinner />
            </div>
          )}

          {!topCharts.loading && topCharts.fetched && topCharts.results.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 px-4 bg-gray-50 dark:bg-gray-900/30 border-2 border-dashed border-gray-200 dark:border-gray-800 rounded-2xl">
              <div className="bg-white dark:bg-gray-800 p-4 rounded-full shadow-sm mb-4 border border-gray-100 dark:border-gray-700">
                <svg
                  className="w-12 h-12 text-blue-500 dark:text-blue-400"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z"
                  />
                </svg>
              </div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2 text-center">
                {t('search.charts.empty')}
              </h3>
              <p className="text-sm text-gray-500 dark:text-gray-400 text-center max-w-sm">
                {t('search.charts.emptyDesc')}
              </p>
            </div>
          )}

          <div className="space-y-2">
            {topCharts.results.map((app, index) => (
              <div
                key={app.id}
                className="flex items-center bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-800 hover:border-gray-300 dark:hover:border-gray-600 transition-colors"
              >
                <Link
                  to={`/search/${app.id}`}
                  state={{ app, country: activeCountry }}
                  className="flex items-center gap-4 flex-1 min-w-0 p-4"
                >
                  <span className="text-sm font-medium text-gray-400 dark:text-gray-500 w-6 text-right shrink-0">
                    {index + 1}
                  </span>
                  <AppIcon url={app.artworkUrl} name={app.name} size="md" />
                  <div className="flex-1 min-w-0">
                    <p className="font-medium text-gray-900 dark:text-white truncate">{app.name}</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400 truncate">
                      {app.artistName}
                    </p>
                    <div className="flex items-center gap-3 mt-1 text-xs text-gray-400 dark:text-gray-500">
                      <span>{app.formattedPrice ?? t('search.free')}</span>
                      <span>{app.primaryGenreName}</span>
                      {app.fileSizeBytes && <span>{formatFileSize(app.fileSizeBytes)}</span>}
                      <StarRating rating={app.averageUserRating} />
                    </div>
                  </div>
                </Link>
                <div className="flex shrink-0 mr-3 gap-1">
                  <Link
                    to={`/search/${app.id}/versions`}
                    state={{ app, country: activeCountry }}
                    className="p-2 text-gray-400 dark:text-gray-500 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                    title={t('search.product.versionHistory')}
                  >
                    <svg
                      className="w-5 h-5"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2}
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z"
                      />
                    </svg>
                  </Link>
                  {hasMatchingAccount && (
                    <button
                      onClick={() => handleQuickDownload(app)}
                      className="p-2 text-gray-400 dark:text-gray-500 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                      title={t('search.product.download')}
                    >
                      <svg
                        className="w-5 h-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
                        />
                      </svg>
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </PageContainer>
  );
}
