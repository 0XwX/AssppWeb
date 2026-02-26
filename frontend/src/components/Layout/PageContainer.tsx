import { ReactNode, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { useSearch } from '../../hooks/useSearch';

interface PageContainerProps {
  title?: string;
  children: ReactNode;
  action?: ReactNode;
}

export default function PageContainer({ title, children, action }: PageContainerProps) {
  const location = useLocation();
  const clearSearch = useSearch((state) => state.clear);

  // 监听路由变化，如果当前路径不在 /search 下，则清空之前的搜索内容
  useEffect(() => {
    if (!location.pathname.startsWith('/search')) {
      clearSearch();
    }
  }, [location.pathname, clearSearch]);

  return (
    <div className="flex-1 overflow-y-auto pb-20 md:pb-0 bg-gray-50 dark:bg-gray-950 transition-colors duration-200">
      <div className="max-w-5xl mx-auto px-4 py-6 sm:px-6">
        {(title || action) && (
          <div className="flex items-center justify-between mb-6">
            {title && <h1 className="text-2xl font-bold text-gray-900 dark:text-white">{title}</h1>}
            {action && <div>{action}</div>}
          </div>
        )}
        {children}
      </div>
    </div>
  );
}
