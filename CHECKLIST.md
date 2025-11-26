# Lapce-Core Editor Migration - Checklist

## ✅ Phase 1: Complete

### Code Implementation
- [x] Create `LapceEditorWidget` in `src/editor_widget.rs` (615 lines)
- [x] Add `LayouterFn` type for syntax highlighting
- [x] Implement line numbers gutter support
- [x] Add `use_lapce_widget: bool` flag to `AdvancedEditor`
- [x] Implement conditional rendering in `src/editor.rs`
- [x] Add settings UI toggle in `src/window_egui.rs`
- [x] Create dummy galley compatibility layer
- [x] Handle layouter borrow checker conflicts

### Testing
- [x] Compilation clean (zero warnings)
- [x] Demo application runs (`cargo run --example lapce_editor_demo`)
- [x] Debug build succeeds
- [x] No regression in existing tests (editor-related)

### Documentation
- [x] User guide: `LAPCE_WIDGET_TOGGLE.md`
- [x] Technical docs: `LAPCE_MIGRATION.md` (updated)
- [x] Implementation summary: `MIGRATION_SUMMARY.md`
- [x] Code comments in conditional block

### Code Quality
- [x] No unused imports
- [x] No unused variables (prefixed with `_`)
- [x] Proper error handling
- [x] Type safety maintained
- [x] Arc<Galley> types correct

## 🔄 Phase 2: Next Steps

### Multi-Cursor Porting
- [ ] Extract multi-cursor rendering logic from `editor.rs`
- [ ] Port to `LapceEditorWidget::show()` method
- [ ] Test Cmd+D (add next occurrence)
- [ ] Test Esc (clear multi-cursor)
- [ ] Remove dummy galley usage in multi-cursor code

### Current Line Highlight
- [ ] Port subtle line highlight to widget
- [ ] Calculate line number from cursor position
- [ ] Draw background rectangle in widget
- [ ] Match VSCode style (subtle gray)

### Autocomplete Integration
- [ ] Test autocomplete popup positioning
- [ ] Fix positioning if broken
- [ ] Test Tab/Enter acceptance
- [ ] Verify focus handling

### User Feedback
- [ ] Beta test with real users
- [ ] Gather performance feedback
- [ ] Document reported issues
- [ ] Prioritize fixes

## ⏳ Phase 3: Future

### Default Switch
- [ ] Change `use_lapce_widget: true` as default
- [ ] Add migration notice in UI
- [ ] Monitor crash reports for 1 week
- [ ] Communicate in release notes

### Cleanup
- [ ] Remove TextEdit code path
- [ ] Remove dummy galley compatibility
- [ ] Remove `use_lapce_widget` flag
- [ ] Simplify conditional rendering

### Performance
- [ ] Add benchmarks (large files)
- [ ] Profile memory usage
- [ ] Optimize rope operations
- [ ] Add metrics to docs

## 📊 Progress

**Overall**: 33% complete

- Phase 1: ✅ 100% (Production integration)
- Phase 2: ⏳ 0% (Feature parity)
- Phase 3: ⏳ 0% (Full migration)

## 🎯 Success Criteria

**Phase 1**: ✅ Met
- Code compiles
- Toggle works
- No regressions
- Docs complete

**Phase 2**: Pending
- Multi-cursor works
- All features ported
- No known bugs
- User acceptance >80%

**Phase 3**: Pending
- TextEdit removed
- Performance improved
- Zero compatibility code
- Documentation updated

## 📝 Notes

- **Risk Level**: Low (toggle allows instant fallback)
- **User Impact**: Opt-in only (zero disruption)
- **Timeline**: Phase 2 target = 2 weeks
- **Blocking**: None (phases independent)

---

Last updated: 2024
Status: ✅ Phase 1 Complete
