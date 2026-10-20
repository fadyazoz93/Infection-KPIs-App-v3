# مواصفات التصميم المعماري لتحسين استهلاك قاعدة البيانات وضمان الترشيد
## Database Optimization & Architecture Guidelines

> **الهدف:** منع الاستنزاف المفرط للقراءات السحابية (Row Reads Exhaustion) وضمان بقاء التطبيق ضمن الحدود المجانية المستقرة لقواعد البيانات السحابية (مثل Turso).

---

### 1. تصميم مخطط قاعدة البيانات (Schema & Indexing Architecture)
* **الفهارس الإلزامية (Mandatory Indexing):**  
  يُحظر إنشاء أي جدول دون تعريف فهارس (`INDEX`) مسبقة على جميع الأعمدة المستخدمة في عمليات التصفية (`WHERE`)، الترتيب (`ORDER BY`)، وحقول الربط (`JOIN`) والمفاتيح الخارجية (`Foreign Keys`).
* **منع المسح الكامل للجداول (No Full Table Scans):**  
  يجب أن تُصمم جميع الاستعلامات لتعتمد على الفهارس حصراً لضمان أن الاستعلام يقرأ فقط السجلات المعنية بدقة بدلاً من فحص آلاف السجلات.

#### التطبيق الفعلي في المشروع (`api/index.ts`):
```sql
-- الفهارس المنشأة تلقائياً عند تهيئة النظام:
CREATE INDEX IF NOT EXISTS idx_kpis_data_month_year ON kpis_data (Month, Year);
CREATE INDEX IF NOT EXISTS idx_kpis_data_month_year_ind ON kpis_data (Month, Year, Indicator);
CREATE INDEX IF NOT EXISTS idx_kpis_data_indicator ON kpis_data (Indicator);
CREATE INDEX IF NOT EXISTS idx_kpis_data_ind_year ON kpis_data (Indicator, Year);
CREATE INDEX IF NOT EXISTS idx_kpis_data_ind_proc ON kpis_data (Indicator, Procedure);
CREATE INDEX IF NOT EXISTS idx_kpis_data_ind_q ON kpis_data (Indicator, Question);
CREATE INDEX IF NOT EXISTS idx_kpis_data_session ON kpis_data (Month, Year, Indicator, Session_Number);
CREATE INDEX IF NOT EXISTS idx_kpis_data_target ON kpis_data (Target_Name, Target_Job_ID);
CREATE INDEX IF NOT EXISTS idx_kpis_data_date ON kpis_data (Date);
```

---

### 2. معمارية طبقة التخزين المؤقت (In-Memory Cache-Aside Pattern)
* **عزل الاستدعاء المباشر:**  
  يُمنع الاتصال المباشر بقاعدة البيانات عند كل طلب مستخدم أو دورة معالجة.
* **دورة عمل القراءة:**  
  يفحص التطبيق الذاكرة العشوائية (`In-Memory Cache`) أولاً:
  - إذا كانت البيانات متوفرة وصالحة، تُعاد فوراً للمستخدم باستهلاك **(0 قراءات سحابية - Zero Cloud Reads)**.
  - عند فقدان الكاش (`Cache Miss`)، يتم طلب البيانات من السحابة وتخزينها في الذاكرة مع تعيين وقت انتهاء صلاحية محدد (`TTL`) يتناسب مع نوع البيانات.

#### التطبيق الفعلي في المشروع:
- **كاش الإعدادات (`cachedSettings`):** مهلة `10 دقائق` (تُحدّث محلياً عند الحفظ).
- **كاش خريطة الإجراءات (`cachedProcMap`):** قراءة فورية من الذاكرة (0 استعلامات).
- **كاش ملخص التقارير (`summaryCache`):** مهلة `دقيقتين` مع تفريغ ذكي عند أي عملية كتابة لمؤشر.
- **كاش حالة الجلسات (`sessionStatusCache`):** مهلة `30 ثانية`.
- **كاش الرسوم البيانية (`dashboardCache`):** مهلة `دقيقتين`.
- **كاش الواجهة الأمامية (`clientReportsSummaryCache` & `clientSessionCache`):** يمنع طلب البيانات المتكرر عند التنقل بين الشاشات.

---

### 3. هيكلة الاستعلامات وضوابط الجلب (Query Optimization & Governance)
* **حظر الجلب العشوائي:**  
  يُمنع منعاً باتاً استخدام `SELECT *`. يجب تحديد أسماء الحقول المطلوبة للعملية فقط في نص الاستعلام.
* **تقييد النتائج والصفحات (Pagination & Limits):**  
  يجب أن تحتوي جميع استعلامات القراءة على سقف محدد للنتائج (`LIMIT`)، مع دعم نظام الترقيم الصفحي لمنع تحميل كتل بيانات ضخمة دفعة واحدة.
* **التجميع السريع عبر SQL:**  
  الاعتماد على استعلامات `GROUP BY` وحساب المجموع والنسب على مستوى المحرك بدلاً من جلب آلاف السجلات الفردية للسيرفر.

#### أمثلة من الكود:
```ts
// 1. طلب أعمدة محددة للإعدادات
SELECT key, value FROM kpis_settings LIMIT 100

// 2. تجميع التقارير في سطر واحد لكل مؤشر/شهر
SELECT Month, Year, Indicator,
       COUNT(*) as totalRecords,
       COUNT(DISTINCT Session_Number) as sessionsCount,
       SUM(CASE WHEN Answer = 'Yes' THEN 1 ELSE 0 END) as totalYes,
       SUM(CASE WHEN Answer IN ('Yes', 'No') THEN 1 ELSE 0 END) as totalValid
FROM kpis_data
GROUP BY Year, Month, Indicator
ORDER BY Year DESC, Month DESC, Indicator ASC
LIMIT 500

// 3. لوحة المؤشرات
SELECT Month, Department, Category, Answer FROM kpis_data WHERE Indicator = ? AND Year = ? LIMIT 10000
```

---

### 4. سياسة الاتصال والمزامنة (Event-Driven vs. Polling Control)
* **منع الفحص الحلقي السريع (No Rapid Polling):**  
  يُحظر نهائياً عمل حلقات تكرار (`Loops / Intervals`) تستعلم من قاعدة البيانات بفواصل زمنية تقل عن 60 ثانية.
* **الاعتماد على الأحداث:**  
  يُصمم تدفق التحديثات بالاعتماد على الأحداث اللحظية أو استدعاءات مقيدة بـ **Debounce** (250ms) عند تفاعل المستخدم بدلاً من توجيه طلبات استعلام متكررة للسحابة.
* **إلغاء المزامنات الثقيلة الخفية:**  
  إلغاء أي عمليات تدقيق أو مزامنة جماعية داخل دوال القراءة (`SELECT`).

---

### 5. التحديث المتفائل ومزامنة الحالة (Optimistic Updates & Local Mutation)
* **منع إعادة القراءة بعد الكتابة:**  
  عند تنفيذ عمليات الإدخال أو التعديل (`INSERT / UPDATE / DELETE`)، يجب تحديث الذاكرة المحلية (`Cache`) وحالة واجهة المستخدم مباشرة بالقيم الجديدة، دون تنفيذ استعلام `SELECT` لاحق للتأكد من وصول البيانات.

#### التطبيق الفعلي في المشروع:
- عند تسجيل تقييم جديد: تُحدّث بيانات الجلسة وسجل التقرير بالذاكرة محلياً فوراً.
- عند تعديل نتائج التقرير: تُحسب النسبة الإجمالية الجديدة محلياً وتُحدّث القائمة في الذاكرة دون طلب إعادة الجلب.
- عند حذف تقرير: يُستبعد السجل من مصفوفة التقارير `rawReportsList` فوراً.
- عند مضاعفة الجلسات: يُحدّث إجمالي الجلسات محلياً في القائمة فوراً.
- عند حفظ الإعدادات: يُحدّث كائن `config` في ذاكرة العميل والخادم مباشرة.
