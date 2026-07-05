package com.example.ui.viewmodel

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import com.example.data.local.AppDatabase
import com.example.data.model.Asset
import com.example.data.model.AssetWithDetails
import com.example.data.model.Department
import com.example.data.model.TransferRecord
import com.example.data.model.TransferWithDetails
import com.example.data.repository.AssetRepository
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

class AssetViewModel(application: Application) : AndroidViewModel(application) {
    private val database = AppDatabase.getDatabase(application)
    private val repository = AssetRepository(
        database.assetDao(),
        database.departmentDao(),
        database.transferDao(),
        database.companyDao()
    )

    // Raw streams from Repository
    val departments: StateFlow<List<Department>> = repository.allDepartments
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val assetsWithDetails: StateFlow<List<AssetWithDetails>> = repository.assetsWithDetails
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val transfersWithDetails: StateFlow<List<TransferWithDetails>> = repository.transfersWithDetails
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    // Filtering options
    private val _searchQuery = MutableStateFlow("")
    val searchQuery = _searchQuery.asStateFlow()

    private val _selectedTypeFilter = MutableStateFlow("ALL") // "ALL", "FIXED", "MOVABLE"
    val selectedTypeFilter = _selectedTypeFilter.asStateFlow()

    private val _selectedDepartmentFilter = MutableStateFlow<Int?>(null)
    val selectedDepartmentFilter = _selectedDepartmentFilter.asStateFlow()

    // Combined filtered assets flow
    val filteredAssets: StateFlow<List<AssetWithDetails>> = combine(
        assetsWithDetails,
        _searchQuery,
        _selectedTypeFilter,
        _selectedDepartmentFilter
    ) { assets, query, type, deptId ->
        assets.filter { item ->
            val matchesQuery = query.isBlank() ||
                item.asset.name.contains(query, ignoreCase = true) ||
                item.asset.serialNumber.contains(query, ignoreCase = true)

            val matchesType = type == "ALL" || item.asset.type == type
            val matchesDept = deptId == null || item.asset.currentDepartmentId == deptId

            matchesQuery && matchesType && matchesDept
        }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    init {
        // Seed default departments if database is empty
        viewModelScope.launch {
            try {
                val depts = repository.allDepartments.first()
                if (depts.isEmpty()) {
                    val radId = repository.insertDepartment(Department(name = "الاشعة", code = "RAD", description = "قسم الأشعة والتصوير الطبي"))
                    val strId = repository.insertDepartment(Department(name = "التعقيم", code = "STR", description = "قسم التعقيم المركزي والتجهيز"))
                    val nurId = repository.insertDepartment(Department(name = "الحضانه والولادة", code = "NUR", description = "قسم الحضانات ورعاية حديثي الولادة"))
                    val inpId = repository.insertDepartment(Department(name = "الرقود", code = "INP", description = "أجنحة الرقود والتنويم الداخلي"))
                    val emrId = repository.insertDepartment(Department(name = "الطوارئ", code = "EMR", description = "قسم طوارئ الحالات الحرجة والإسعاف"))
                    val surId = repository.insertDepartment(Department(name = "العمليات", code = "SUR", description = "غرف العمليات الجراحية والتخدير"))
                    val icuId = repository.insertDepartment(Department(name = "العناية المركزة", code = "ICU", description = "وحدة العناية المركزة ومراقبة المرضى"))
                    val labId = repository.insertDepartment(Department(name = "المختبر", code = "LAB", description = "مختبر التحاليل الطبية وعينات الدم"))
                    repository.insertDepartment(Department(name = ".E.N.T عيادة", code = "ENT", description = "عيادة الأنف والأذن والحنجرة"))
                    repository.insertDepartment(Department(name = "عيادة الأطفال", code = "PED", description = "عيادة طب الأطفال والمتابعة"))
                    repository.insertDepartment(Department(name = "عيادة الاسنان", code = "DEN", description = "عيادة طب وجراحة الأسنان"))
                    repository.insertDepartment(Department(name = "عيادة الباطنية", code = "INT", description = "عيادة الأمراض الباطنية والجهاز الهضمي"))
                    repository.insertDepartment(Department(name = "عيادة التحصين", code = "IMM", description = "عيادة التطعيمات والتحصين الوقائي"))
                    repository.insertDepartment(Department(name = "عيادة الجراحة العامه", code = "GEN", description = "عيادة الجراحة العامة والاستشارات"))
                    repository.insertDepartment(Department(name = "عيادة الجلد", code = "DER", description = "عيادة الأمراض الجلدية والعناية"))
                    repository.insertDepartment(Department(name = "عيادة العظام", code = "ORT", description = "عيادة جراحة العظام والمفاصل"))
                    repository.insertDepartment(Department(name = "عيادة العلاج الطبيعي", code = "PHT", description = "عيادة التأهيل والعلاج الطبيعي"))
                    repository.insertDepartment(Department(name = "عيادة العيون", code = "OPH", description = "عيادة طب وجراحة العيون والشبكية"))
                    repository.insertDepartment(Department(name = "عيادة المخ والاعصاب", code = "NEU", description = "عيادة طب المخ والأعصاب والتخطيط"))
                    val carId = repository.insertDepartment(Department(name = "عيادة القلب", code = "CAR", description = "عيادة فحص القلب وتخطيط الجهد"))
                    repository.insertDepartment(Department(name = "عيادة المسالك", code = "URO", description = "عيادة جراحة المسالك البولية والكلى"))
                    repository.insertDepartment(Department(name = "عيادة النساء", code = "GYN", description = "عيادة النساء والولادة ومتابعة الحمل"))
                    repository.insertDepartment(Department(name = "عيادة جراحة القلب", code = "CAS", description = "عيادة استشارات وجراحة القلب المفتوح"))
                    repository.insertDepartment(Department(name = "مركز القلب", code = "HCT", description = "المركز التخصصي لأبحاث وعلاج القلب"))

                    // Seed realistic medical assets
                    repository.insertAsset(
                        Asset(
                            id = "AST-RAD-01",
                            name = "جهاز أشعة مقطعية CT Scan 128 Slices",
                            serialNumber = "CT-GE-99812",
                            type = "BIOMEDICAL",
                            description = "جهاز تصوير مقطعي متطور مزود بتقنية تقليل جرعة الإشعاع والتصوير السريع.",
                            purchaseDate = System.currentTimeMillis() - 31536000000L,
                            currentDepartmentId = radId.toInt(),
                            status = "ACTIVE",
                            condition = "EXCELLENT",
                            model = "GE Revolution CT",
                            quantity = 1,
                            imageUri = null,
                            accessories = "طاولة فحص، وحدة تحكم، كابلات ضغط عالي",
                            manufacturer = "GE Healthcare"
                        )
                    )

                    repository.insertAsset(
                        Asset(
                            id = "AST-ICU-02",
                            name = "جهاز مراقبة وظائف الأعضاء Patient Monitor",
                            serialNumber = "MON-PHI-4412",
                            type = "BIOMEDICAL",
                            description = "جهاز مراقبة العلامات الحيوية المستمر مع شاشة لمس 15 بوصة وإنذار ذكي.",
                            purchaseDate = System.currentTimeMillis() - 15768000000L,
                            currentDepartmentId = icuId.toInt(),
                            status = "ACTIVE",
                            condition = "NEW",
                            model = "IntelliVue MX800",
                            quantity = 6,
                            imageUri = null,
                            accessories = "كابل تخطيط قلب، مستشعر أكسجين، سوار ضغط",
                            manufacturer = "Philips Medical"
                        )
                    )

                    repository.insertAsset(
                        Asset(
                            id = "AST-EMR-03",
                            name = "جهاز تنفس صناعي متطور Ventilator",
                            serialNumber = "VEN-DRG-8812",
                            type = "BIOMEDICAL",
                            description = "جهاز تنفس صناعي للحالات الحرجة والطوارئ مزود بأنظمة تحكم بالضغط والحجم.",
                            purchaseDate = System.currentTimeMillis() - 25000000000L,
                            currentDepartmentId = emrId.toInt(),
                            status = "ACTIVE",
                            condition = "GOOD",
                            model = "Evita V500",
                            quantity = 4,
                            imageUri = null,
                            accessories = "أنابيب تنفس، فلاتر بكتيرية، ذراع حامل",
                            manufacturer = "Dräger"
                        )
                    )

                    repository.insertAsset(
                        Asset(
                            id = "AST-STR-04",
                            name = "جهاز تعقيم مركزي بالبخار Autoclave",
                            serialNumber = "STR-GET-1123",
                            type = "BIOMEDICAL",
                            description = "وحدة تعقيم بالبخار سعة 500 لتر مخصصة للأدوات الجراحية ومعدات غرف العمليات.",
                            purchaseDate = System.currentTimeMillis() - 40000000000L,
                            currentDepartmentId = strId.toInt(),
                            status = "ACTIVE",
                            condition = "EXCELLENT",
                            model = "Getinge HS66",
                            quantity = 2,
                            imageUri = null,
                            accessories = "سلال تحميل ستانلس ستيل، عربة نقل، طابعة تقارير",
                            manufacturer = "Getinge"
                        )
                    )

                    repository.insertAsset(
                        Asset(
                            id = "AST-SUR-05",
                            name = "طاولة عمليات جراحية كهربائية Operative Table",
                            serialNumber = "SUR-MAQ-7731",
                            type = "BIOMEDICAL",
                            description = "طاولة جراحة متعددة الحركات تعمل بالتحكم الكهربائي الدقيق لمختلف التخصصات.",
                            purchaseDate = System.currentTimeMillis() - 10000000000L,
                            currentDepartmentId = surId.toInt(),
                            status = "ACTIVE",
                            condition = "NEW",
                            model = "Maquet Magnus",
                            quantity = 3,
                            imageUri = null,
                            accessories = "مساند أذرع، حوامل أرجل، وحدة تحكم لاسلكية",
                            manufacturer = "Maquet / Getinge"
                        )
                    )
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    // Filter adjustments
    fun updateSearchQuery(query: String) {
        _searchQuery.value = query
    }

    fun updateTypeFilter(type: String) {
        _selectedTypeFilter.value = type
    }

    fun updateDepartmentFilter(deptId: Int?) {
        _selectedDepartmentFilter.value = deptId
    }

    // Actions
    fun addAsset(
        id: String,
        name: String,
        serialNumber: String,
        type: String,
        description: String,
        currentDepartmentId: Int,
        condition: String,
        model: String = "",
        quantity: Int = 1,
        imageUri: String? = null,
        accessories: String = "",
        manufacturer: String = "",
        status: String = "ACTIVE"
    ) {
        viewModelScope.launch {
            val asset = Asset(
                id = id,
                name = name,
                serialNumber = serialNumber,
                type = type,
                description = description,
                purchaseDate = System.currentTimeMillis(),
                currentDepartmentId = currentDepartmentId,
                status = status,
                condition = condition,
                model = model,
                quantity = quantity,
                imageUri = imageUri,
                accessories = accessories,
                manufacturer = manufacturer
            )
            repository.insertAsset(asset)
        }
    }

    // Excel-compatible CSV Importer
    fun importAssetsFromCsv(csvText: String, onComplete: (Int, String) -> Unit) {
        viewModelScope.launch {
            try {
                val lines = csvText.lines()
                if (lines.isEmpty() || lines.first().isBlank()) {
                    onComplete(0, "الملف فارغ أو غير صالح")
                    return@launch
                }
                
                // Get header and columns
                val header = lines.first().split(",").map { it.trim().removeSurrounding("\"") }
                
                var importedCount = 0
                val departmentsList = repository.allDepartments.first()
                val defaultDept = departmentsList.firstOrNull()
                val defaultDeptId = defaultDept?.id ?: 1
                
                // Map to store department name (trimmed lowercase) to its ID for avoiding duplicates
                val departmentsMap = departmentsList.associate { 
                    it.name.trim().lowercase() to it.id 
                }.toMutableMap()
                
                for (i in 1 until lines.size) {
                    val line = lines[i].trim()
                    if (line.isBlank()) continue
                    
                    // CSV column values
                    val tokens = line.split(",").map { it.trim().removeSurrounding("\"") }
                    if (tokens.isEmpty()) continue
                    
                    // Map indices dynamically based on header
                    fun getValueForHeader(vararg names: String): String {
                        val index = header.indexOfFirst { headerName -> 
                            names.any { it.equals(headerName, ignoreCase = true) }
                        }
                        return if (index != -1 && index < tokens.size) tokens[index] else ""
                    }
                    
                    val nameValue = getValueForHeader("الاسم", "name", "Device Name")
                    if (nameValue.isBlank()) continue
                    
                    val serial = getValueForHeader("الرقم التسلسلي", "serialNumber", "Serial Number")
                    val type = getValueForHeader("النوع", "type", "Type")
                    val description = getValueForHeader("الوصف", "description", "Notes")
                    val condition = getValueForHeader("الجودة", "condition", "Condition")
                    val model = getValueForHeader("الموديل", "model", "Model")
                    val quantity = getValueForHeader("الكمية", "quantity", "Quantity").toIntOrNull() ?: 1
                    val assetCode = getValueForHeader("كود تعريفي", "assetCode", "Asset ID").ifBlank { getValueForHeader("Asset ID", "id") }
                    val idValue = assetCode.ifBlank { System.currentTimeMillis().toString() + "-" + importedCount }
                    val accessories = getValueForHeader("الملحقات", "accessories", "Accessories")
                    val manufacturerIdOrName = getValueForHeader("الشركة المصنعة", "manufacturer", "Manufacturer")
                    val companyName = getValueForHeader("الشركة", "company", "Company")
                    
                    // Use company name if available, otherwise fallback to manufacturer
                    val manufacturer = if (companyName.isNotBlank()) companyName else manufacturerIdOrName
                    
                    // Check if department exists by name, otherwise use default
                    val deptName = getValueForHeader("القسم", "department", "Department").trim()
                    var deptId = defaultDeptId
                    if (deptName.isNotBlank()) {
                        val deptKey = deptName.lowercase()
                        if (departmentsMap.containsKey(deptKey)) {
                            deptId = departmentsMap[deptKey]!!
                        } else {
                            // Create department dynamically
                            val newDeptId = repository.insertDepartment(
                                Department(name = deptName, code = deptName.take(3).uppercase(), description = "تم إنشاؤه تلقائياً")
                            )
                            deptId = newDeptId.toInt()
                            departmentsMap[deptKey] = deptId
                        }
                    }
                    
                    val asset = Asset(
                        id = idValue,
                        name = nameValue,
                        serialNumber = serial,
                        type = type,
                        description = description,
                        purchaseDate = System.currentTimeMillis(),
                        currentDepartmentId = deptId,
                        status = "ACTIVE",
                        condition = condition,
                        model = model,
                        quantity = quantity,
                        imageUri = null,
                        accessories = accessories,
                        manufacturer = manufacturer
                    )
                    repository.insertAsset(asset)
                    importedCount++
                }
                onComplete(importedCount, "تم استيراد $importedCount من الأصول بنجاح!")
            } catch (e: Exception) {
                onComplete(0, "خطأ في قراءة ملف CSV: ${e.localizedMessage}")
            }
        }
    }

    // Excel-compatible CSV Exporter
    fun exportAssetsToCsv(): String {
        val builder = StringBuilder()
        // CSV Header
        builder.append("الاسم,كود تعريفي,الشركة المصنعة,الموديل,الرقم التسلسلي,النوع,التصنيف,الملحقات,الوصف,التكلفة,الكمية,الجودة,القسم\n")
        
        val assets = filteredAssets.value
        val depts = departments.value.associateBy { it.id }
        for (item in assets) {
            val name = item.asset.name.replace(",", " ")
            val code = item.asset.id.replace(",", " ")
            val mfg = item.asset.manufacturer.replace(",", " ")
            val model = item.asset.model.replace(",", " ")
            val serial = item.asset.serialNumber.replace(",", " ")
            val type = item.asset.type
            val acc = item.asset.accessories.replace(",", " ")
            val description = item.asset.description.replace(",", " ")
            val quantity = item.asset.quantity
            val condition = item.asset.condition
            val deptName = depts[item.asset.currentDepartmentId]?.name ?: "غير محدد"
            builder.append("\"$name\",\"$code\",\"$mfg\",\"$model\",\"$serial\",\"$type\",\"$acc\",\"$description\",$quantity,\"$condition\",\"$deptName\"\n")
        }
        return builder.toString()
    }

    // CSV Empty Template for Excel
    fun getCsvTemplate(): String {
        val builder = StringBuilder()
        builder.append("الاسم,كود تعريفي,الشركة المصنعة,الموديل,الرقم التسلسلي,النوع,التصنيف,الملحقات,الوصف,التكلفة,الكمية,الجودة,القسم\n")
        builder.append("\"طاولة مكتبية ذكية\",\"AST-1001\",\"IKEA\",\"IKEA-Desk-X\",\"SN-99238\",\"MOVABLE\",\"أثاث مكتب\",\"\",\"طاولة مكتبية قابلة لتعديل الارتفاع\",1250,5,\"NEW\",\"الموارد البشرية\"\n")
        builder.append("\"شاشة حاسوب 27 بوصة\",\"AST-1002\",\"LG\",\"LG-27UL\",\"SN-88231\",\"MOVABLE\",\"أجهزة إلكترونية\",\"كابل طاقة - كابل HDMI\",\"شاشة عرض بدقة 4K فائقة الوضوح\",1500,10,\"EXCELLENT\",\"تقنية المعلومات\"\n")
        return builder.toString()
    }

    fun updateAsset(asset: Asset) {
        viewModelScope.launch {
            repository.updateAsset(asset)
        }
    }

    fun deleteAsset(asset: Asset) {
        viewModelScope.launch {
            repository.deleteAsset(asset)
        }
    }

    fun addDepartment(name: String, code: String, description: String) {
        viewModelScope.launch {
            repository.insertDepartment(
                Department(
                    name = name,
                    code = code,
                    description = description
                )
            )
        }
    }

    fun deleteDepartment(department: Department) {
        viewModelScope.launch {
            repository.deleteDepartment(department)
        }
    }

    fun transferAsset(
        assetId: String,
        fromDeptId: Int,
        toDeptId: Int,
        authorizedBy: String,
        notes: String,
        onComplete: (Boolean) -> Unit = {}
    ) {
        viewModelScope.launch {
            val success = repository.transferAsset(
                assetId = assetId,
                fromDeptId = fromDeptId,
                toDeptId = toDeptId,
                authorizedBy = authorizedBy,
                notes = notes
            )
            onComplete(success)
        }
    }
}
